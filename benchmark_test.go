package nnut

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"
)

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

const userCount = 10000

// TestSetupBenchmarkDB creates a template database for benchmarks
func TestSetupBenchmarkDB(t *testing.T) {
	os.Remove("benchmark_template.db")
	os.Remove("benchmark_template.db.wal")
	db, err := Open("benchmark_template.db")
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	// Create diverse test data for realistic benchmarking
	commonNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ryan"}
	for index := 0; index < userCount; index++ {
		key := fmt.Sprintf("user_%d", index)
		name := commonNames[index%len(commonNames)]
		email := fmt.Sprintf("%s%d@example.com", name, index)
		age := rand.Intn(63) + 18 // Ages 18-80
		testUser := TestUser{UUID: key, Name: name, Email: email, Age: age}
		err := store.Put(context.Background(), testUser)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()
	os.Remove("benchmark_template.db.wal") // Remove WAL after flush to avoid replay issues
	// Leave the DB file behind for benchmarks to copy
}

func BenchmarkGet(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		key := fmt.Sprintf("user_%d", iteration%userCount)
		_, err := store.Get(context.Background(), key)
		if err != nil {
			b.Fatalf("Failed to get: %v", err)
		}
	}
}

func BenchmarkBatchGet(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Standardize batch size for fair performance comparison
	batchSize := 100
	b.ResetTimer()
	for batchIndex := 0; batchIndex < b.N/batchSize; batchIndex++ {
		var keys []string
		for keyIndex := 0; keyIndex < batchSize; keyIndex++ {
			keys = append(keys, fmt.Sprintf("user_%d", (batchIndex*batchSize+keyIndex)%userCount))
		}
		_, err := store.GetBatch(context.Background(), keys)
		if err != nil {
			b.Fatalf("Failed to get batch: %v", err)
		}
	}
}

func BenchmarkPut(b *testing.B) {
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user_%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(context.Background(), user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()
}

func BenchmarkBatchPut(b *testing.B) {
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Using a batch size allows us to compare directly with BenchmarkPut
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var users []TestUser
		for j := 0; j < batchSize; j++ {
			key := fmt.Sprintf("user_%d", (i*batchSize+j)%b.N)
			user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
			users = append(users, user)
		}
		err := store.PutBatch(context.Background(), users)
		if err != nil {
			b.Fatalf("Failed to put batch: %v", err)
		}
	}
	db.Flush()
}

func BenchmarkDelete(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user_%d", i%userCount)
		err := store.Delete(context.Background(), key)
		if err != nil {
			b.Fatalf("Failed to delete: %v", err)
		}
	}
	db.Flush()
}

func BenchmarkBatchDelete(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Using a batch size allows us to compare directly with BenchmarkDelete
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var keys []string
		for j := 0; j < batchSize; j++ {
			keys = append(keys, fmt.Sprintf("user_%d", (i*batchSize+j)%userCount))
		}
		err := store.DeleteBatch(context.Background(), keys)
		if err != nil {
			b.Fatalf("Failed to delete batch: %v", err)
		}
	}
	db.Flush()
}

// BenchmarkHighLoadConcurrent simulates high concurrent load with mixed operations
func BenchmarkHighLoadConcurrent(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	db, err := Open("benchmark.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// High load: concurrent Puts, Gets, and Queries
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := 0
		for pb.Next() {
			key := fmt.Sprintf("load_%d_%d", index%10, index)
			index++

			// Mix of operations
			switch index % 3 {
			case 0: // Put
				user := TestUser{UUID: key, Name: "New", Email: "new@example.com", Age: 29}
				store.Put(context.Background(), user)
			case 1: // Get
				store.Get(context.Background(), fmt.Sprintf("user_%d", index%userCount))
			case 2: // Query
				store.Query(context.Background(), &Query{
					Conditions: []Condition{{Field: "Name", Value: "Alice"}},
					Limit:      10,
				})
			}
		}
	})
	db.Flush()
}

// BenchmarkWALTruncation measures the performance of WAL truncation after flush
func BenchmarkWALTruncation(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}

	config := &Config{
		WALFlushSize:     1024,
		WALFlushInterval: time.Hour, // Prevent auto-flush
		MaxBufferBytes:   100000,    // Large buffer
	}
	db, err := OpenWithConfig("benchmark.db", config)
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("benchmark.db")
	defer os.Remove("benchmark.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Add some operations to buffer
	for i := 0; i < 100; i++ {
		user := TestUser{UUID: fmt.Sprintf("trunc_%d", i), Name: "Trunc", Email: "trunc@example.com", Age: i}
		store.Put(context.Background(), user)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Flush and truncate
		db.Flush()
	}
}
