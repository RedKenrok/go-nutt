package nnut

import (
	"os"
	"path/filepath"
	"testing"
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
	err = store.Put(testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	store.database.Flush()

	retrieved, err := store.Get("key1")
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

	_, err = store.Get("nonexistent")
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
	err = store.Put(testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Verify record exists before deletion
	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get before delete: %v", err)
	}
	if retrieved.Name != testUser.Name {
		t.Fatal("Data mismatch before delete")
	}

	// Remove the record
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	db.Flush()

	// Confirm record is no longer accessible
	_, err = store.Get("key1")
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
	err = store.PutBatch(testUsers)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}
	db.Flush()

	// Retrieve multiple records simultaneously
	retrievedResults, err := store.GetBatch([]string{"1", "2", "4"})
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
	err = store.DeleteBatch([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}
	db.Flush()

	// Verify only expected records remain
	retrievedResults, err = store.GetBatch([]string{"1", "2", "3"})
	if err != nil {
		t.Fatalf("Failed to get batch after delete: %v", err)
	}
	if len(retrievedResults) != 1 || retrievedResults["2"].Name != "Bob" {
		t.Fatal("Wrong results after batch delete")
	}
}
