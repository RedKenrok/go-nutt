package nnut

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestConcurrency tests basic concurrent Put and Get operations.
// Run with -race to detect data races: go test -race -run TestConcurrency
func TestConcurrency(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1,
		WALFlushInterval: time.Hour,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	numberOfGoroutines := 10
	numberOfOperations := 10

	// Simulate concurrent write operations to test thread safety
	for goroutineIndex := 0; goroutineIndex < numberOfGoroutines; goroutineIndex++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()
			for operationIndex := 0; operationIndex < numberOfOperations; operationIndex++ {
				key := fmt.Sprintf("key%d_%d", goroutineId, operationIndex)
				testUser := TestUser{UUID: key, Name: "User", Email: "email"}
				store.Put(context.Background(), testUser)
			}
		}(goroutineIndex)
	}

	// Simulate concurrent read operations to test thread safety
	for goroutineIndex := 0; goroutineIndex < numberOfGoroutines; goroutineIndex++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()
			for operationIndex := 0; operationIndex < numberOfOperations; operationIndex++ {
				key := fmt.Sprintf("key%d_%d", goroutineId, operationIndex)
				store.Get(context.Background(), key) // ignore error, may not be flushed yet
			}
		}(goroutineIndex)
	}

	wg.Wait()

	// Ensure all operations are persisted before verification
	db.Flush()

	for goroutineIndex := 0; goroutineIndex < numberOfGoroutines; goroutineIndex++ {
		for operationIndex := 0; operationIndex < numberOfOperations; operationIndex++ {
			key := fmt.Sprintf("key%d_%d", goroutineIndex, operationIndex)
			_, err := store.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
		}
	}
}

// TestRaceConditionsFlush tests for race conditions during concurrent Flush calls and operations.
// Run with -race: go test -race -run TestRaceConditionsFlush
func TestRaceConditionsFlush(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1000, // Large to avoid auto-flush
		WALFlushInterval: time.Hour,
		MaxBufferBytes:   10000,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 20
	opsPerGoroutine := 50

	// Concurrent Puts
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key%d_%d", id, j)
				user := TestUser{UUID: key, Name: "Name", Email: "email"}
				store.Put(context.Background(), user)
			}
		}(i)
	}

	// Concurrent Flush calls
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 10; k++ {
				db.Flush()
				time.Sleep(1 * time.Millisecond) // Small delay to interleave
			}
		}()
	}

	wg.Wait()
	db.Flush() // Final flush

	// Verify data
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < opsPerGoroutine; j++ {
			key := fmt.Sprintf("key%d_%d", i, j)
			_, err := store.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
		}
	}
}

// TestRaceConditionsIndexUpdates tests for race conditions in index updates during concurrent operations.
// Run with -race: go test -race -run TestRaceConditionsIndexUpdates
func TestRaceConditionsIndexUpdates(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1,
		WALFlushInterval: time.Hour,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 20

	// Concurrent Puts and Deletes on indexed fields
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key%d_%d", id, j%10) // Reuse keys for deletes
				if j%2 == 0 {
					user := TestUser{UUID: key, Name: fmt.Sprintf("Name%d", j), Email: fmt.Sprintf("email%d@example.com", j), Age: j}
					store.Put(context.Background(), user)
				} else {
					store.Delete(context.Background(), key)
				}
			}
		}(i)
	}

	wg.Wait()
	db.Flush()

	// Basic verification (exact state may vary due to concurrency)
	results, err := store.Query(context.Background(), &Query{Limit: 100})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("No results after concurrent operations")
	}
}

// TestConcurrentQueries tests concurrent query operations with ongoing writes.
// Run with -race: go test -race -run TestConcurrentQueries
func TestConcurrentQueries(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     10,
		WALFlushInterval: time.Hour,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: fmt.Sprintf("Name%d", i%10), Email: fmt.Sprintf("email%d@example.com", i), Age: i % 50}
		store.Put(context.Background(), user)
	}
	db.Flush()

	var wg sync.WaitGroup
	numQueryGoroutines := 10
	numWriteGoroutines := 5
	queriesPerGoroutine := 20
	writesPerGoroutine := 10

	// Concurrent queries
	for i := 0; i < numQueryGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				_, err := store.Query(context.Background(), &Query{
					Conditions: []Condition{{Field: "Name", Value: "Name1"}},
					Limit:      10,
				})
				if err != nil {
					t.Errorf("Query failed: %v", err)
				}
			}
		}()
	}

	// Concurrent writes
	for i := 0; i < numWriteGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				key := fmt.Sprintf("new_user%d_%d", id, j)
				user := TestUser{UUID: key, Name: "NewName", Email: "new@example.com", Age: 25}
				store.Put(context.Background(), user)
			}
		}(i)
	}

	wg.Wait()
}

// TestConcurrentBatchOperations tests concurrent batch operations.
// Run with -race: go test -race -run TestConcurrentBatchOperations
func TestConcurrentBatchOperations(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     50,
		WALFlushInterval: time.Hour,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 8
	batchSize := 10

	// Concurrent batch puts
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var users []TestUser
			for j := 0; j < batchSize; j++ {
				key := fmt.Sprintf("batch%d_%d", id, j)
				user := TestUser{UUID: key, Name: "BatchName", Email: "batch@example.com", Age: 30}
				users = append(users, user)
			}
			err := store.PutBatch(context.Background(), users)
			if err != nil {
				t.Errorf("PutBatch failed: %v", err)
			}
		}(i)
	}

	// Concurrent batch deletes (on different keys)
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var keys []string
			for j := 0; j < batchSize/2; j++ {
				key := fmt.Sprintf("batch%d_%d", id+numGoroutines, j) // Different keys
				keys = append(keys, key)
			}
			// First put some to delete
			for _, k := range keys {
				user := TestUser{UUID: k, Name: "Temp", Email: "temp@example.com", Age: 20}
				store.Put(context.Background(), user)
			}
			err := store.DeleteBatch(context.Background(), keys)
			if err != nil {
				t.Errorf("DeleteBatch failed: %v", err)
			}
		}(i)
	}

	wg.Wait()
	db.Flush()

	// Verify some data exists
	results, err := store.Query(context.Background(), &Query{Limit: 50})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("No results after batch operations")
	}
}
