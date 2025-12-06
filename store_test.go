package nnut

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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
	if store == nil {
		t.Fatal("Store is nil")
	}
}

func TestPutAndGet(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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

	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	store.database.Flush()

	retrieved, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved.Name != testUser.Name || retrieved.Email != testUser.Email {
		t.Fatalf("Retrieved data mismatch: got %+v, want %+v", retrieved, testUser)
	}
}

func TestGetNonExistent(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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

	_, err = store.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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

	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Verify record exists before deletion
	retrieved, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get before delete: %v", err)
	}
	if retrieved.Name != testUser.Name {
		t.Fatal("Data mismatch before delete")
	}

	// Remove the record
	err = store.Delete(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	db.Flush()

	// Confirm record is no longer accessible
	_, err = store.Get(context.Background(), "key1")
	if err == nil {
		t.Fatal("Expected error after delete")
	}
}

func TestBatchOperations(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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

	testUsers := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com"},
	}
	err = store.PutBatch(context.Background(), testUsers)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}
	db.Flush()

	// Retrieve multiple records simultaneously
	retrievedResults, err := store.GetBatch(context.Background(), []string{"1", "2", "4"})
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}
	if len(retrievedResults) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(retrievedResults))
	}
	if retrievedResults["1"].Name != "Alice" || retrievedResults["2"].Name != "Bob" {
		t.Fatal("Wrong batch get results")
	}

	// Remove multiple records in one operation
	err = store.DeleteBatch(context.Background(), []string{"1", "3"})
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}
	db.Flush()

	// Verify only expected records remain
	retrievedResults, err = store.GetBatch(context.Background(), []string{"1", "2", "3"})
	if err != nil {
		t.Fatalf("Failed to get batch after delete: %v", err)
	}
	if len(retrievedResults) != 1 || retrievedResults["2"].Name != "Bob" {
		t.Fatal("Wrong results after batch delete")
	}
}

func TestBufferAwareReading(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	db, err := Open(dbPath)
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

	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Get should return data from buffer without flushing
	retrieved, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get from buffer: %v", err)
	}
	if retrieved.Name != testUser.Name || retrieved.Email != testUser.Email {
		t.Fatalf("Retrieved data mismatch: got %+v, want %+v", retrieved, testUser)
	}

	// Test buffer-aware batch get
	testUser2 := TestUser{UUID: "key2", Name: "Jane", Email: "jane@example.com"}
	err = store.Put(context.Background(), testUser2)
	if err != nil {
		t.Fatalf("Failed to put second user: %v", err)
	}

	results, err := store.GetBatch(context.Background(), []string{"key1", "key2", "nonexistent"})
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results["key1"].Name != "John" || results["key2"].Name != "Jane" {
		t.Fatal("Wrong batch results")
	}
}

func TestBufferDeduplication(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1000,
		WALFlushInterval: time.Hour,
		MaxBufferBytes:   10000, // Large enough to hold multiple operations
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

	// Put same key multiple times - should only keep latest
	key := "testkey"
	user1 := TestUser{UUID: key, Name: "First", Email: "first@example.com"}
	user2 := TestUser{UUID: key, Name: "Second", Email: "second@example.com"}
	user3 := TestUser{UUID: key, Name: "Third", Email: "third@example.com"}

	err = store.Put(context.Background(), user1)
	if err != nil {
		t.Fatalf("Failed to put first: %v", err)
	}
	err = store.Put(context.Background(), user2)
	if err != nil {
		t.Fatalf("Failed to put second: %v", err)
	}
	err = store.Put(context.Background(), user3)
	if err != nil {
		t.Fatalf("Failed to put third: %v", err)
	}

	// Get should return the latest version
	retrieved, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if retrieved.Name != "Third" {
		t.Fatalf("Expected latest version, got %s", retrieved.Name)
	}
}

func TestWALReplayWithBuffer(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1000,
		WALFlushInterval: time.Hour,
		MaxBufferBytes:   1000,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add some data and force flush to create WAL
	testUser := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Close and reopen to test WAL replay
	db.Close()

	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Data should be replayed from WAL
	retrieved, err := store2.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get after replay: %v", err)
	}
	if retrieved.Name != testUser.Name {
		t.Fatalf("WAL replay failed: got %s, want %s", retrieved.Name, testUser.Name)
	}
}
