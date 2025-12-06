package nnut

import (
	"context"
	"os"
	"testing"
)

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

	// Test typical query patterns
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		_, err := store.Query(context.Background(), &Query{
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
		_, err := store.Query(context.Background(), &Query{
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
		_, err := store.Query(context.Background(), &Query{
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
		_, err := store.Query(context.Background(), &Query{
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

func BenchmarkQueryCount(b *testing.B) {
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
	// Count users with a specific name
	for i := 0; i < b.N; i++ {
		_, err := store.QueryCount(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
		})
		if err != nil {
			b.Fatalf("Failed to query count: %v", err)
		}
	}
}

func BenchmarkQueryCountIndex(b *testing.B) {
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
	// Count all users using index
	for i := 0; i < b.N; i++ {
		_, err := store.QueryCount(context.Background(), &Query{
			Index: "name",
		})
		if err != nil {
			b.Fatalf("Failed to query count index: %v", err)
		}
	}
}

func BenchmarkQueryNoConditions(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query no conditions: %v", err)
		}
	}
}

func BenchmarkQueryNonIndexedField(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Age", Value: 25, Operator: GreaterThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query non-indexed: %v", err)
		}
	}
}

func BenchmarkQueryComplexOperators(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "A", Operator: GreaterThanOrEqual},
				{Field: "Age", Value: 30, Operator: LessThan},
			},
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query complex operators: %v", err)
		}
	}
}

func BenchmarkQueryLargeLimit(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Limit: 10000,
		})
		if err != nil {
			b.Fatalf("Failed to query large limit: %v", err)
		}
	}
}

func BenchmarkQueryOffsetOnly(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Name", Value: "Alice"},
			},
			Offset: 50,
		})
		if err != nil {
			b.Fatalf("Failed to query offset only: %v", err)
		}
	}
}

func BenchmarkQuerySortingAscending(b *testing.B) {
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
		_, err := store.Query(context.Background(), &Query{
			Index: "name",
			Sort:  Ascending,
			Limit: 100,
		})
		if err != nil {
			b.Fatalf("Failed to query sorting ascending: %v", err)
		}
	}
}

func BenchmarkQueryCountNoConditions(b *testing.B) {
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
		_, err := store.QueryCount(context.Background(), &Query{})
		if err != nil {
			b.Fatalf("Failed to query count no conditions: %v", err)
		}
	}
}

func BenchmarkQueryCountNonIndexed(b *testing.B) {
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
		_, err := store.QueryCount(context.Background(), &Query{
			Conditions: []Condition{
				{Field: "Age", Value: 25, Operator: GreaterThan},
			},
		})
		if err != nil {
			b.Fatalf("Failed to query count non-indexed: %v", err)
		}
	}
}
