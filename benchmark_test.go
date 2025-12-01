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

	// Pre-populate users with varied names, emails, and ages
	commonNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ryan"}
	for i := 0; i < userCount; i++ {
		key := fmt.Sprintf("user_%d", i)
		name := commonNames[i%len(commonNames)]
		email := fmt.Sprintf("%s%d@example.com", name, i)
		age := rand.Intn(63) + 18 // Ages 18-80
		user := TestUser{UUID: key, Name: name, Email: email, Age: age}
		err := store.Put(user)
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
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user_%d", i%userCount)
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

	// Using a batch size allows us to compare directly with BenchmarkGet
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var keys []string
		for j := 0; j < batchSize; j++ {
			keys = append(keys, fmt.Sprintf("user_%d", (i*batchSize+j)%userCount))
		}
		_, err := store.GetBatch(keys)
		if err != nil {
			b.Fatalf("Failed to get batch: %v", err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}
	if _, err := os.Stat("benchmark_template.db.wal"); err == nil {
		copyFile("benchmark_template.db.wal", "benchmark.db.wal")
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

	// Query for a common name, simulating real searches
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Query(&Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkQueryMultipleConditions(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}
	if _, err := os.Stat("benchmark_template.db.wal"); err == nil {
		copyFile("benchmark_template.db.wal", "benchmark.db.wal")
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
		_, err := store.Query(&Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
				{Field: "Age", Value: 30, Operator: GreaterThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkQuerySorting(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}
	if _, err := os.Stat("benchmark_template.db.wal"); err == nil {
		copyFile("benchmark_template.db.wal", "benchmark.db.wal")
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
	// Query with sorting by name
	for i := 0; i < b.N; i++ {
		_, err := store.Query(&Query{
			Index: "name",
			Sort:  Descending,
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkQueryLimitOffset(b *testing.B) {
	// Copy template database
	os.Remove("benchmark.db")
	os.Remove("benchmark.db.wal")
	err := copyFile("benchmark_template.db", "benchmark.db")
	if err != nil {
		b.Fatalf("Failed to copy template DB: %v", err)
	}
	if _, err := os.Stat("benchmark_template.db.wal"); err == nil {
		copyFile("benchmark_template.db.wal", "benchmark.db.wal")
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
	// Query with limit and offset
	for i := 0; i < b.N; i++ {
		_, err := store.Query(&Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Offset: 50,
			Limit:  50,
		})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
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
