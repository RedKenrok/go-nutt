package nnut

import (
	"context"
	"fmt"
	"io"
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
		MaxBufferBytes:   1000000, // Large buffer to prevent flush during puts
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

func TestWALChecksumVerification(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1024,
		WALFlushInterval: time.Hour,
		MaxBufferBytes:   100000,
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

	// Add data
	user := TestUser{UUID: "checksum_test", Name: "Checksum", Email: "checksum@example.com", Age: 30}
	store.Put(context.Background(), user)

	// Close without flush
	db.Close()

	// Corrupt WAL by flipping a byte in the checksum
	walPath := dbPath + ".wal"
	file, err := os.OpenFile(walPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	data := make([]byte, 100)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read WAL: %v", err)
	}
	if n > 10 {
		// Flip a byte in the checksum area (last 4 bytes of entry)
		pos := n - 5
		data[pos] ^= 1
		file.Seek(int64(pos), 0)
		file.Write([]byte{data[pos]})
	}
	file.Close()

	// Reopen - should detect checksum mismatch and discard WAL
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB after checksum corruption: %v", err)
	}
	defer db2.Close()

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Data should not be recovered (WAL discarded)
	_, err = store2.Get(context.Background(), "checksum_test")
	if err == nil {
		t.Fatal("Data should not be recovered after checksum corruption")
	}
}

func TestWALTruncation(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), t.Name()+".db")
	config := &Config{
		WALFlushSize:     1024,
		WALFlushInterval: time.Hour, // Prevent auto-flush
		MaxBufferBytes:   100000,    // Large buffer to prevent auto-flush
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

	// Add initial data
	for i := 0; i < 10; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "TruncationTest", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// Check WAL has content
	walPath := dbPath + ".wal"
	stat, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	initialSize := stat.Size()
	if initialSize == 0 {
		t.Fatal("WAL should have content before flush")
	}

	// Flush manually
	db.Flush()

	// Wait for truncation
	time.Sleep(100 * time.Millisecond)

	// WAL should be truncated (empty, since all committed)
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatal("WAL should be empty after flush of all operations")
	}

	// Add more data
	for i := 10; i < 15; i++ {
		user := TestUser{UUID: fmt.Sprintf("user%d", i), Name: "TruncationTest", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	// WAL should have content again
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("WAL should have content after new operations")
	}

	// Flush again
	db.Flush()

	// Wait
	time.Sleep(100 * time.Millisecond)

	// WAL should be empty again
	stat, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should exist: %v", err)
	}
	if stat.Size() != 0 {
		t.Fatal("WAL should be empty after second flush")
	}

	// Close and reopen to test recovery
	db.Close()
	db2, err := OpenWithConfig(dbPath, config)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	store2, err := NewStore[TestUser](db2, "users")
	if err != nil {
		t.Fatalf("Failed to create store after reopen: %v", err)
	}

	// All data should be present
	for i := 0; i < 15; i++ {
		retrieved, err := store2.Get(context.Background(), fmt.Sprintf("user%d", i))
		if err != nil {
			t.Fatalf("Failed to get user%d: %v", i, err)
		}
		if retrieved.Name != "TruncationTest" {
			t.Fatalf("Data incorrect for user%d", i)
		}
	}
}
