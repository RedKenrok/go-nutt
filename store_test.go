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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	store.database.Flush()

	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if retrieved.Name != user.Name || retrieved.Email != user.Email {
		t.Fatalf("Retrieved data mismatch: got %+v, want %+v", retrieved, user)
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

	user := TestUser{UUID: "key1", Name: "John", Email: "john@example.com"}
	err = store.Put(user)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	db.Flush()

	// Check exists
	retrieved, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get before delete: %v", err)
	}
	if retrieved.Name != user.Name {
		t.Fatal("Data mismatch before delete")
	}

	// Delete
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	db.Flush()

	// Check not exists
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com"},
		{UUID: "2", Name: "Bob", Email: "bob@example.com"},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com"},
	}
	err = store.PutBatch(users)
	if err != nil {
		t.Fatalf("Failed to put batch: %v", err)
	}
	db.Flush()

	// Get batch
	results, err := store.GetBatch([]string{"1", "2", "4"})
	if err != nil {
		t.Fatalf("Failed to get batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results["1"].Name != "Alice" || results["2"].Name != "Bob" {
		t.Fatal("Wrong batch get results")
	}

	// Delete batch
	err = store.DeleteBatch([]string{"1", "3"})
	if err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}
	db.Flush()

	// Check remaining
	results, err = store.GetBatch([]string{"1", "2", "3"})
	if err != nil {
		t.Fatalf("Failed to get batch after delete: %v", err)
	}
	if len(results) != 1 || results["2"].Name != "Bob" {
		t.Fatal("Wrong results after batch delete")
	}
}

func TestQuery(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query by name
	results, err := store.Query(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Name != "Alice" {
			t.Fatal("Wrong result")
		}
	}
}

func TestQueryMultipleConditions(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
		{UUID: "4", Name: "Charlie", Email: "charlie@example.com", Age: 40},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with multiple conditions: Name == "Alice" AND Age > 30
	results, err := store.Query(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].UUID != "3" {
		t.Fatal("Wrong result")
	}
}

func TestQuerySorting(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "2", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "3", Name: "Bob", Email: "bob@example.com", Age: 25},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query sorted by name ascending
	results, err := store.Query(&Query{
		Index: "name",
		Sort:  Ascending,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[1].Name != "Bob" || results[2].Name != "Charlie" {
		t.Fatal("Wrong sort order")
	}

	// Query sorted by name descending
	results, err = store.Query(&Query{
		Index: "name",
		Sort:  Descending,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if results[0].Name != "Charlie" || results[1].Name != "Bob" || results[2].Name != "Alice" {
		t.Fatal("Wrong sort order")
	}
}

func TestQueryLimitOffset(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Query with limit
	results, err := store.Query(&Query{
		Index: "name",
		Sort:  Ascending,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0].Name != "Alice" || results[1].Name != "Bob" {
		t.Fatal("Wrong results")
	}

	// Query with offset
	results, err = store.Query(&Query{
		Index:  "name",
		Sort:   Ascending,
		Offset: 1,
		Limit:  2,
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0].Name != "Bob" || results[1].Name != "Charlie" {
		t.Fatal("Wrong results")
	}
}

func TestQueryOperators(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Charlie", Email: "charlie@example.com", Age: 40},
		{UUID: "4", Name: "David", Email: "david@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Test GreaterThan on Age
	results, err := store.Query(&Query{
		Conditions: []Condition{
			{Field: "Age", Value: 30, Operator: GreaterThan},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	// Should be Charlie (40) and David (35)
	ages := []int{results[0].Age, results[1].Age}
	if !((ages[0] == 35 && ages[1] == 40) || (ages[0] == 40 && ages[1] == 35)) {
		t.Fatal("Wrong results for GreaterThan")
	}

	// Test LessThanOrEqual on Name
	results, err = store.Query(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Bob", Operator: LessThanOrEqual},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	// Alice and Bob
}

func TestQueryCount(t *testing.T) {
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

	users := []TestUser{
		{UUID: "1", Name: "Alice", Email: "alice@example.com", Age: 30},
		{UUID: "2", Name: "Bob", Email: "bob@example.com", Age: 25},
		{UUID: "3", Name: "Alice", Email: "alice2@example.com", Age: 35},
	}
	for _, u := range users {
		err = store.Put(u)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	}
	db.Flush()

	// Count with condition
	count, err := store.QueryCount(&Query{
		Conditions: []Condition{
			{Field: "Name", Value: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	if count != 2 {
		t.Fatalf("Expected count 2, got %d", count)
	}
}
