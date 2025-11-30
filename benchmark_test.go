package nnut

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate data
	itemCount := int(math.Sqrt(float64(b.N)));
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("key%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%itemCount)
		_, err := store.Get(key)
		if err != nil {
			b.Fatalf("Failed to get: %v", err)
		}
	}
}

func BenchmarkBatchGet(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate data
	itemCount := int(math.Sqrt(float64(b.N)));
	for i := 0; i < itemCount; i++ {
		key := fmt.Sprintf("key%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Using a batch size allows us to compare directly with BenchmarkGet
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var keys []string
		for j := 0; j < batchSize; j++ {
			keys = append(keys, fmt.Sprintf("key%d", (i*batchSize+j)%itemCount))
		}
		_, err := store.GetBatch(keys)
		if err != nil {
			b.Fatalf("Failed to get batch: %v", err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate realistic data: 10,000 users with varied names, emails, and ages
	// Simulate real-world diversity: common names, unique emails, random ages
	commonNames := []string{"John", "Jane", "Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack", "Kate", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Ryan"}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("user_%d", i)
		name := commonNames[i%len(commonNames)]
		email := fmt.Sprintf("%s%d@example.com", name, i)
		age := rand.Intn(63) + 18 // Ages 18-80
		user := TestUser{UUID: key, Name: name, Email: email, Age: age}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	b.ResetTimer()
	// Query for a common name, simulating real searches
	for i := 0; i < b.N; i++ {
		_, err := store.Query(&Query{Index: "name", Value: "John", Operator: Equals, Offset: 0, Limit: 100})
		if err != nil {
			b.Fatalf("Failed to query: %v", err)
		}
	}
}

func BenchmarkPut(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()
}

func BenchmarkBatchPut(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

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
		  key := fmt.Sprintf("key%d", (i*batchSize+j)%b.N)
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
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		err := store.Delete(key)
		if err != nil {
			b.Fatalf("Failed to delete: %v", err)
		}
	}
	db.Flush()
}

func BenchmarkBatchDelete(b *testing.B) {
	os.Remove("bench.db")
	os.Remove("bench.db.wal")
	db, err := Open("bench.db")
	if err != nil {
		b.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()
	defer os.Remove("bench.db")
	defer os.Remove("bench.db.wal")

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		b.Fatalf("Failed to create store: %v", err)
	}

	// Pre-populate data
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		user := TestUser{UUID: key, Name: "John", Email: "john@example.com", Age: 30}
		err := store.Put(user)
		if err != nil {
			b.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Using a batch size allows us to compare directly with BenchmarkDelete
	batchSize := 100
	b.ResetTimer()
	for i := 0; i < b.N/batchSize; i++ {
		var keys []string
		for j := 0; j < batchSize; j++ {
			keys = append(keys, fmt.Sprintf("key%d", (i*batchSize+j)%b.N))
		}
		err := store.DeleteBatch(keys)
		if err != nil {
			b.Fatalf("Failed to delete batch: %v", err)
		}
	}
	db.Flush()
}
