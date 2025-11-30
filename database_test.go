package nnut

import (
	"fmt"
	"os"
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
	os.Remove("test.db")
	os.Remove("test.db.wal")
	db, err := Open("test.db")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("test.db")
	defer os.Remove("test.db.wal")
}

func TestNewStore(t *testing.T) {
	db, err := Open("test.db")
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
	if store == nil {
		t.Fatal("Store is nil")
	}
}

func TestPutAndGet(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.db.wal")
	db, err := Open("test.db")
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	store.database.Flush()

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved.Name != user.Name || retrieved.Email != user.Email {
		t.Fatalf("Retrieved data mismatch: got %+v, want %+v", retrieved, user)
	}
}

func TestGetNonExistent(t *testing.T) {
	db, err := Open("test.db")
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

	_, err = store.Get("nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}
}

func TestDelete(t *testing.T) {
	db, err := Open("test.db")
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Check exists
	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get before delete: %v", err)
	}
	if retrieved.Name != user.Name {
		t.Fatal("Data mismatch before delete")
	}

	// Delete
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	db.Flush()

	// Check not exists
	_, err = store.Get("key1")
	if err == nil {
		t.Fatal("Expected error after delete")
	}
}

func TestQuery(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.db.wal")
	db, err := Open("test.db")
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com"},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query by name
	results, err := store.Query(&Query{Index: "name", Value: "Alice", Operator: Equals})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Name != "Alice" {
			t.Fatal("Wrong result")
		}
	}
}

func TestBatchOperations(t *testing.T) {
	db, err := Open("test.db")
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com"},
	}
	err = store.PutBatch(users)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}
	db.Flush()

	// Get batch
	results, err := store.GetBatch([]string{"1", "2", "4"})
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results["1"].Name != "Alice" || results["2"].Name != "Bob" {
		t.Fatal("Wrong batch get results")
	}

	// Delete batch
	err = store.DeleteBatch([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}
	db.Flush()

	// Check remaining
	results, err = store.GetBatch([]string{"1", "2", "3"})
	if err != nil {
		t.Fatalf("Failed to get batch after delete: %v", err)
	}
	if len(results) != 1 || results["2"].Name != "Bob" {
		t.Fatal("Wrong results after batch delete")
	}
}

func TestWALBufferSize(t *testing.T) {
	config := &Config{
		WALBufferSize:    2,
		WALFlushInterval: time.Hour, // long to not auto flush
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

	// Put 3 items, buffer size 2, so should flush after 2nd, but since Put appends and checks after, wait.
	user1 := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	user2 := TestUser{UUID: "key2", Name: "Jane", Email: "jane@example.com"}
	err = store.Put(user2)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// After 2, buffer full, should flush
	time.Sleep(100 * time.Millisecond) // wait for flush

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if retrieved.Name != user1.Name {
		t.Fatal("key1 not flushed")
	}

	user3 := TestUser{UUID: "key3", Name: "Bob", Email: "bob@example.com"}
	err = store.Put(user3)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// Now buffer has 1, not full
	_, err = store.Get("key3")
	if err == nil {
		t.Fatal("key3 should not be flushed yet")
	}
}

func TestWALFlushInterval(t *testing.T) {
	config := &Config{
		WALBufferSize:    100,
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Wait for interval
	time.Sleep(100 * time.Millisecond)

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get after interval: %v", err)
	}
	if retrieved.Name != user.Name {
		t.Fatal("Not flushed by interval")
	}
}

func TestConcurrency(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.db.wal")
	config := &Config{
		WALBufferSize:    1,
		WALFlushInterval: time.Hour,
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

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 10

	// Writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key%d_%d", id, j)
				user := TestUser{UUID: key, Name: "User", Email: "email"}
				store.Put(user)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := fmt.Sprintf("key%d_%d", id, j)
				store.Get(key) // ignore error, may not be flushed yet
			}
		}(i)
	}

	wg.Wait()

	// Flush and check
	db.Flush()

	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOps; j++ {
			key := fmt.Sprintf("key%d_%d", i, j)
			_, err := store.Get(key)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
		}
	}
}
