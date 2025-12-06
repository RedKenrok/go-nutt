package nnut

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	err = store.Put(context.Background(), firstUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	secondUser := TestUser{UUID: "key2", Name: "Jane", Email: "jane@example.com"}
	err = store.Put(context.Background(), secondUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// Buffer reaches capacity, triggering automatic flush
	time.Sleep(100 * time.Millisecond) // wait for flush

	retrieved, err := store.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if retrieved.Name != firstUser.Name {
		t.Fatal("key1 not flushed")
	}

	thirdUser := TestUser{UUID: "key3", Name: "Bob", Email: "bob@example.com"}
	err = store.Put(context.Background(), thirdUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	// With buffer-aware reading, key3 should be retrievable from buffer
	retrieved3, err := store.Get(context.Background(), "key3")
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
	err = store.Put(context.Background(), testUser)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Allow time for periodic flush to occur
	time.Sleep(100 * time.Millisecond)

	retrieved, err := store.Get(context.Background(), "key1")
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
		err = store.Put(context.Background(), user)
		if err != nil {
			t.Fatalf("Failed to put user %d: %v", i, err)
		}
	}

	// Give time for flush to occur
	time.Sleep(100 * time.Millisecond)

	// Check that data is persisted (flush occurred)
	retrieved, err := store.Get(context.Background(), "user0")
	if err != nil {
		t.Fatalf("Failed to get user0: %v", err)
	}
	if retrieved.Name != "Name0" {
		t.Fatal("Data not flushed properly")
	}
}

// TestWALRecoveryAfterSimulatedCrash tests WAL recovery by simulating a crash and reopening.
func TestWALRecoveryAfterSimulatedCrash(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1000, // Delay flush
		WALFlushInterval: time.Hour,
		MaxBufferBytes:   10000,
	}
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Add data without flushing
	for i := 0; i < 50; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "CrashTest", Email: "crash@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// "Simulate crash" by closing without flushing
	db.Close()

	// Reopen - should replay WAL
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB after crash: %v", err)
	}
	defer db2.Close()
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store after reopen: %v", err)
	}

	// Data should be recovered
	for i := 0; i < 50; i++ {
		retrieved, err := store2.Get(context.Background(), fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatalf("Failed to get user%d: %v", i, err)
		}
		if retrieved.Name != "CrashTest" {
			t.Fatalf("Data not recovered for user%d", i)
		}
	}
}

// TestWALCorruptionHandling tests handling of corrupted WAL files.
func TestWALCorruptionHandling(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1,
		WALFlushInterval: time.Hour,
	}

	// Create DB and add data
	db, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	user := TestUser{UUID: "test", Name: "Test", Email: "test@example.com", Age: 25}
	store.Put(context.Background(), user)
	db.Flush()
	db.Close()

	// Corrupt the WAL by truncating it
	walPath := dbPath + ".wal"
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	file.Truncate(10) // Corrupt by shortening
	file.Close()

	// Reopen - should handle corruption gracefully (discard WAL)
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen after corruption: %v", err)
	}
	defer db2.Close()
	defer os.Remove(dbPath)
	defer os.Remove(walPath)

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Data should still be there (from DB, not WAL)
	retrieved, err := store2.Get(context.Background(), "test")
	if err != nil {
		t.Fatalf("Failed to get after corruption handling: %v", err)
	}
	if retrieved.Name != "Test" {
		t.Fatal("Data lost after WAL corruption")
	}
}
