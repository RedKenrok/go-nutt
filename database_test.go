package nnut

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestUser for testing
type TestUser struct {
	UUID  string `nnut:"key"`
	Name  string `nnut:"index:name"`
	Email string `nnut:"index:email"`
	Age   int    `nnut:"index:age"`
}

func TestOpen(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")
}

func TestWALFlushSize(t *testing.T) {
	config := &Config{
		WALFlushSize:     1000,      // large count to not trigger
		WALFlushInterval: time.Hour, // long to not auto flush
		MaxBufferBytes:   1000,      // size to allow 2 operations but not 3
	}
	db, err := OpenWithConfig("test.db", config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Trigger flush by exceeding buffer size to verify WAL behavior
	firstUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(firstUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	secondUser := TestUser{UUID: "key2", Name: "Jane", Email: "jane@example.com"}
	err = store.Put(secondUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// Buffer reaches capacity, triggering automatic flush
	time.Sleep(100 * time.Millisecond) // wait for flush

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if retrieved.Name != firstUser.Name {
		t.Fatal("key1 not flushed")
	}

	thirdUser := TestUser{UUID: "key3", Name: "Bob", Email: "bob@example.com"}
	err = store.Put(thirdUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// With buffer-aware reading, key3 should be retrievable from buffer
	retrieved3, err := store.Get("key3")
	if err != nil {
		t.Fatalf("Failed to get key3: %v", err)
	}
	if retrieved3.Name != thirdUser.Name {
		t.Fatal("key3 not retrievable from buffer")
	}
}

func TestWALFlushInterval(t *testing.T) {
	config := &Config{
		WALFlushSize:     100,
		WALFlushInterval: 50 * time.Millisecond,
	}
	db, err := OpenWithConfig("test.db", config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Allow time for periodic flush to occur
	time.Sleep(100 * time.Millisecond)

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get after interval: %v", err)
	}
	if retrieved.Name != testUser.Name {
		t.Fatal("Not flushed by interval")
	}
}

func TestSizeBasedFlush(t *testing.T) {
	config := &Config{
		WALFlushSize:     1000,      // Large count to not trigger
		WALFlushInterval: time.Hour, // Long to not trigger
		MaxBufferBytes:   1000,      // Small size to trigger flush
	}
	db, err := OpenWithConfig("test.db", config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add operations until buffer size triggers flush
	for i := 0; i < 10; i++ {
		user := TestUser{
			UUID:  fmt.Sprintf("user%d", i),
			Name:  fmt.Sprintf("Name%d", i),
			Email: fmt.Sprintf("email%d@example.com", i),
		}
		err = store.Put(user)
		if err != nil {
			t.Fatalf("Failed to put user %d: %v", i, err)
		}
	}

	// Give time for flush to occur
	time.Sleep(100 * time.Millisecond)

	// Check that data is persisted (flush occurred)
	retrieved, err := store.Get("user0")
	if err != nil {
		t.Fatalf("Failed to get user0: %v", err)
	}
	if retrieved.Name != "Name0" {
		t.Fatal("Data not flushed properly")
	}
}

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
				store.Put(testUser)
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
				store.Get(key) // ignore error, may not be flushed yet
			}
		}(goroutineIndex)
	}

	wg.Wait()

	// Ensure all operations are persisted before verification
	db.Flush()

	for goroutineIndex := 0; goroutineIndex < numberOfGoroutines; goroutineIndex++ {
		for operationIndex := 0; operationIndex < numberOfOperations; operationIndex++ {
			key := fmt.Sprintf("key%d_%d", goroutineIndex, operationIndex)
			_, err := store.Get(key)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
		}
	}
}
