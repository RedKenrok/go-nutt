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
		WALFlushSize:     2,
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
