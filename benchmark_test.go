package nnut

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
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
		err := store.Put(testUser)
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
		_, err := store.Get(key)
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
		_, err := store.GetBatch(keys)
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
		err := store.Put(user)
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
		err := store.PutBatch(users)
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
		err := store.Delete(key)
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
		err := store.DeleteBatch(keys)
		if err != nil {
			b.Fatalf("Failed to delete batch: %v", err)
		}
	}
	db.Flush()
}
