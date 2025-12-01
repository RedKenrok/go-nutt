# nnut

> Still very much a work in progress.

A Go library that wraps [go.etcd.io/bbolt](https://github.com/etcd-io/bbolt/#readme) to provide enhanced embedded key-value storage capabilities. It adds a Write Ahead Log (WAL) to reduce write frequency and implements automatic encoding using [msgpack](github.com/vmihailenco/msgpack/#readme) and indexing.

## Features

- **bbolt wrapper**: Builds on top of the reliable bbolt database for embedded key-value storage;
- **Write Ahead Log (WAL)**: Reduces disk write frequency by buffering changes in a log before committing to the database;
- **Automatic indices**: Maintains and updates indices automatically as data is inserted, updated, or deleted;
<!--- **Automatic encryption**: Marked fields are automatically encrypted when inserted, updated, or deleted and decrypted when retrieved.-->

## Installation

```bash
go get github.com/redkenrok/go-nnut
```

## Configuration

The library supports various configuration options for customization:

```go
import "github.com/redkenrok/nnut"

config := &nnut.Config{
  WALFlushSize: 1024, // Flushes when mutations count exceeds
  WALFlushInterval: time.Minute * 15, // Flushes every 15 minutes
  BBoltOptions: &bolt.Options{
    Timeout: time.Second * 10,
    ReadOnly: false,
  },
}

db, err := nnut.OpenWithConfig("mydata.db", config)
if err != nil {
  log.Fatal(err)
}
```
<!--EncryptionAlgorithm: "your-encryption-algorithm",
EncryptionKey: []byte("your-32-byte-encryption-key"),  -->

<!--- **EncryptionAlgorithm**: [...]
- **EncryptionKey**: 32-byte key for encrypting marked fields-->
- **WALFlushSize**: Size of the mutations buffer
- **WALFlushInterval**: How often to flush WAL to disk
- **BBoltOptions**: Standard bbolt database options (timeout, read-only mode, etc.)

## Usage

The library provides fundamental key-value storage operations, directly wrapping `bbolt` for reliable embedded database functionality. It implements advanced typed data storage with automatic features like indexing and encryption, leveraging Go generics and struct tags for metadata-driven behavior.

First you define your Go structs with special tags to specify additional data such as the unique key of the data. The library uses reflection to interpret these tags and apply the appropriate behavior.

```go
type User struct {
  UUID  string `nnut:"key"`
  Email string
}
```

You can then create a type-safe store instance for your data structures. This will handle serialization and automatic feature application.

```go
func main() {
  db, err := nnut.Open("mydata.db")
  if err != nil {
    log.Fatal(err)
  }
  defer db.Close()

  // Create a store for User type
  userStore, err := nnut.NewStore[User](db, "users")
  if err != nil {
    log.Fatal(err)
  }
}
```

You can then perform type-safe create, read, update, and delete operations.

```go
// Create or update a user record
user := User{
 	UUID: "aa0000a0...",
 	Email: "ron@example.com",
}
err = userStore.Put(user)
if err != nil {
  log.Fatal(err)
}

// Read a user by primary key
user, err = userStore.Get("aa0000a0...")
if err != nil {
  log.Fatal(err)
}
log.Printf("User: %+v", user)

// Delete a user by primary key
err = userStore.Delete("aa0000a0...")
if err != nil {
  log.Fatal(err)
}
```

### Batch operations

For better performance with multiple operations, use batch methods instead.

```go
// Batch put multiple users
users := []User{
  {UUID: "uuid1", Email: "user1@example.com"},
  {UUID: "uuid2", Email: "user2@example.com"},
}
err = userStore.PutBatch(users)
if err != nil {
  log.Fatal(err)
}

// Batch get by keys
users, err = userStore.GetBatch([]string{"uuid1", "uuid2"})
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}

// Batch delete by keys
err = userStore.DeleteBatch([]string{"uuid1", "uuid2"})
if err != nil {
  log.Fatal(err)
}
```

### Query

Status: in progress

You can specify indexes on the data structure and the typed container will automatically ensure the indexes are kept up to date. You can then sort and paginate over this index.

```go
type User struct {
  UUID  string `nnut:"key"`
  Email string `nnut:"index:email"`
}

[...]

// Query users in alphabetical order of their email adresses
query := &nnut.Query{
  Index: "Email",
  Offset: 0,
  Limit: 48,
  Sort: nnut.Ascending, // or nnut.Descending to iterate in reverse alphabetical order
}

users, err := userStore.Query(query)
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}
```

#### Query logic

Status: in progress

```go
// Get a user by their e-mail
query := &nnut.Query{
  Conditions: []nutt.Condition{
    {Field: "Email", Value: "ron@example.com"},
  },
  // [...]
}
users, err := userStore.Query(query)
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}
```

Query data by multiple fields using multiple conditions:

```go
// Get users where email equals "ron@example.com" AND age is greater than 28
query := &nnut.Query{
	Conditions: []nnut.Condition{
	  {Field: "email", Value: "ron@example.com"},
	  {Field: "age", Value: 28, Operator: nnut.GreaterThan},
	},
	// [...]
}

users, err := userStore.Query(query)
if err != nil {
  log.Fatal(err)
}
for _, user := range users {
  log.Printf("User: %+v", user)
}
```

Supported operators:
- **Equals**: Exact match
- **GreaterThan**: Value greater than specified
- **LessThan**: Value less than specified
- **GreaterThanOrEqual**: Value greater than or equal to specified
- **LessThanOrEqual**: Value less than or equal to specified

#### Query count

To get the number of records matching a query without retrieving the data:

```go
// Count users with a specific age
query := &nnut.Query{
  Conditions: []nnut.Condition{
    {Field: "Age", Value: 28},
  },
}
count, err := userStore.QueryCount(query)
if err != nil {
  log.Fatal(err)
}
log.Printf("Found %d users with that email", count)
```

<!--
### Encryption

Status: planned

To store information securily fields can be marked as encrypted. The field will then automatically be encrypted before storing and decrypted when retrieved.

```go
type User struct {
	UUID  string `nnut:"key"`
	Email string `nnut:"index:email"`
  Name  string `nnut:"encrypt"`
}
````

To ensure the same data does not encrypt to the same value when it appears in multiple records a salt can be specified. This should reference the name of another field containing the salt value. This will be added during the encryption to ensure no two entries are alike. Do keep in mind that salted fields can **not** be indexed.

```go
type User struct {
 	UUID  string `nnut:"key"`
 	Email string `nnut:"index:email"`
  Name  string `nnut:"encrypt:Salt"`
  Salt  string
}
```

The salt value is also automatically filled in if left empty and it needs to be used for encryption.
-->

## Benchmarks

```
~ go test -bench=. -benchtime=2s
goos: darwin
goarch: amd64
pkg: github.com/redkenrok/go-nutt
cpu: Intel(R) Core(TM) i5-1038NG7 CPU @ 2.00GHz
BenchmarkGet-8                       	 1423807	      1645 ns/op
BenchmarkBatchGet-8                  	 1988394	      1200 ns/op
BenchmarkQuery-8                     	  131059	     18219 ns/op
BenchmarkQueryMultipleConditions-8   	  122750	     18058 ns/op
BenchmarkQuerySorting-8              	  103797	     22136 ns/op
BenchmarkQueryLimitOffset-8          	  141386	     17515 ns/op
BenchmarkPut-8                       	    9303	    233514 ns/op
BenchmarkBatchPut-8                  	   55838	     43277 ns/op
BenchmarkDelete-8                    	   10848	    223286 ns/op
BenchmarkBatchDelete-8               	   52473	     42947 ns/op
```

### Improvements

- Try to update indices lazily reducing the Put and Delete overhead.
