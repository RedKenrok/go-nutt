package nnut

import (
	"bytes"
	"errors"
	"reflect"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Operators
const (
  Equals = iota
)

// Sorting orders
const (
  Unsorted = iota
  Ascending
  Descending
)

type Condition struct {
  Field    string
	Value    interface{}
	Operator int
}

// Query for queries
type Query struct {
  Index  string
	Offset int // Number of results to skip
	Limit  int // Maximum number of results to return (0 = no limit)
	Sort   int // Order to sort the items in either unsorted, asceding, or descending

	Conditions []Condition
}

// Store represents a typed bucket
type Store[T any] struct {
	database    *DB
	bucket      []byte
	keyField    int            // index of the field tagged with nnut:"key"
	indexFields map[string]int // index name -> field index
}

// NewStore creates a new store for type T with the given bucket name
func NewStore[T any](db *DB, bucketName string) (*Store[T], error) {
	// Use reflection to find the key field
	var zero T
	typ := reflect.TypeOf(zero)
	if typ.Kind() != reflect.Struct {
		return nil, errors.New("type must be a struct")
	}
	keyField := -1
	indexFields := make(map[string]int)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("nnut")
		if tag == "key" {
			if field.Type.Kind() != reflect.String {
				return nil, errors.New("key field must be string")
			}
			keyField = i
		} else if strings.HasPrefix(tag, "index:") {
			parts := strings.Split(tag, ":")
			if len(parts) == 2 {
				indexName := parts[1]
				indexFields[indexName] = i
			}
		}
	}
	if keyField == -1 {
		return nil, errors.New("no field tagged with nnut:\"key\"")
	}
	return &Store[T]{
		database:    db,
		bucket:      []byte(bucketName),
		keyField:    keyField,
		indexFields: indexFields,
	}, nil
}

// Get retrieves a value by key
func (s *Store[T]) Get(key string) (T, error) {
	var result T
	err := s.database.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(key))
		if data == nil {
			return errors.New("key not found")
		}
		return msgpack.Unmarshal(data, &result)
	})
	return result, err
}

// GetBatch retrieves multiple values by keys
func (s *Store[T]) GetBatch(keys []string) (map[string]T, error) {
	result := make(map[string]T)
	err := s.database.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if b == nil {
			// Bucket not found, return empty
			return nil
		}
		for _, key := range keys {
			data := b.Get([]byte(key))
			if data != nil {
				var item T
				err := msgpack.Unmarshal(data, &item)
				if err != nil {
					continue
				}
				result[key] = item
			}
		}
		return nil
	})
	return result, err
}

// extractIndexValues extracts index field values from the struct
func (s *Store[T]) extractIndexValues(value T) map[string]string {
	v := reflect.ValueOf(value)
	result := make(map[string]string)
	for name, idx := range s.indexFields {
		fieldVal := v.Field(idx)
		if fieldVal.Kind() == reflect.String {
			result[name] = fieldVal.String()
		}
	}
	return result
}

// Put stores a value
func (s *Store[T]) Put(value T) error {
	// Extract key from value using reflection
	v := reflect.ValueOf(value)
	key := v.Field(s.keyField).String()
	if key == "" {
		return errors.New("key field is empty")
	}

	// Read old value if exists to get old index values
	var oldIndexValues map[string]string
	old, err := s.Get(key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(old)
	} else {
		oldIndexValues = make(map[string]string)
	}

	newIndexValues := s.extractIndexValues(value)

	// Create index ops
	var indexOps []indexOperation
	for name := range s.indexFields {
		oldVal := oldIndexValues[name]
		newVal := newIndexValues[name]
		if oldVal != newVal {
			indexOps = append(indexOps, indexOperation{
				IndexName: name,
				OldValue:  oldVal,
				NewValue:  newVal,
			})
		}
	}

	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}

	op := operation{
		Bucket:          s.bucket,
		Key:             key,
		Value:           data,
		IsPut:           true,
		IndexOperations: indexOps,
	}

	// Write to WAL file
	walEnc := msgpack.NewEncoder(s.database.walFile)
	err = walEnc.Encode(op)
	if err != nil {
		return err
	}

	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, op)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALBufferSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}
	return nil
}

// PutBatch stores multiple values
func (s *Store[T]) PutBatch(values []T) error {
	// Extract keys
	keys := make([]string, len(values))
	keyToValue := make(map[string]T)
	for i, value := range values {
		v := reflect.ValueOf(value)
		key := v.Field(s.keyField).String()
		if key == "" {
			return errors.New("key field is empty")
		}
		keys[i] = key
		keyToValue[key] = value
	}

	// Batch get old values
	oldValues, err := s.GetBatch(keys)
	if err != nil {
		return err
	}

	// Prepare operations
	var ops []operation
	for _, key := range keys {
		value := keyToValue[key]
		old, exists := oldValues[key]
		var oldIndexValues map[string]string
		if exists {
			oldIndexValues = s.extractIndexValues(old)
		} else {
			oldIndexValues = make(map[string]string)
		}

		newIndexValues := s.extractIndexValues(value)

		// Create index ops
		var indexOps []indexOperation
		for name := range s.indexFields {
			oldVal := oldIndexValues[name]
			newVal := newIndexValues[name]
			if oldVal != newVal {
				indexOps = append(indexOps, indexOperation{
					IndexName: name,
					OldValue:  oldVal,
					NewValue:  newVal,
				})
			}
		}

		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		err = enc.Encode(value)
		if err != nil {
			return err
		}
		data := buf.Bytes()

		op := operation{
			Bucket:          s.bucket,
			Key:             key,
			Value:           data,
			IsPut:           true,
			IndexOperations: indexOps,
		}
		ops = append(ops, op)
	}

	// Batch write to WAL
	var walBuf bytes.Buffer
	walEnc := msgpack.NewEncoder(&walBuf)
	for _, op := range ops {
		err = walEnc.Encode(op)
		if err != nil {
			return err
		}
	}
	_, err = s.database.walFile.Write(walBuf.Bytes())
	if err != nil {
		return err
	}

	// Add to buffer
	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, ops...)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALBufferSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}

// Delete removes a value by key
func (s *Store[T]) Delete(key string) error {
	// Read old value to get old index values
	var oldIndexValues map[string]string
	old, err := s.Get(key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(old)
	} else {
		oldIndexValues = make(map[string]string)
	}

	// Create index ops
	var indexOps []indexOperation
	for name := range s.indexFields {
		oldVal := oldIndexValues[name]
		if oldVal != "" {
			indexOps = append(indexOps, indexOperation{
				IndexName: name,
				OldValue:  oldVal,
				NewValue:  "",
			})
		}
	}

	op := operation{
		Bucket:          s.bucket,
		Key:             key,
		Value:           nil,
		IsPut:           false,
		IndexOperations: indexOps,
	}

	// Write to WAL file
	walEnc := msgpack.NewEncoder(s.database.walFile)
	err = walEnc.Encode(op)
	if err != nil {
		return err
	}

	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, op)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALBufferSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}
	return nil
}

// DeleteBatch removes multiple values by keys
func (s *Store[T]) DeleteBatch(keys []string) error {
	// Batch get old values to extract index values
	oldValues, err := s.GetBatch(keys)
	if err != nil {
		return err
	}

	// Prepare operations
	var ops []operation
	for _, key := range keys {
		old, exists := oldValues[key]
		var oldIndexValues map[string]string
		if exists {
			oldIndexValues = s.extractIndexValues(old)
		} else {
			oldIndexValues = make(map[string]string)
		}

		// Create index ops
		var indexOps []indexOperation
		for name := range s.indexFields {
			oldVal := oldIndexValues[name]
			if oldVal != "" {
				indexOps = append(indexOps, indexOperation{
					IndexName: name,
					OldValue:  oldVal,
					NewValue:  "",
				})
			}
		}

		op := operation{
			Bucket:          s.bucket,
			Key:             key,
			Value:           nil,
			IsPut:           false,
			IndexOperations: indexOps,
		}
		ops = append(ops, op)
	}

	// Batch write to WAL
	var walBuf bytes.Buffer
	walEnc := msgpack.NewEncoder(&walBuf)
	for _, op := range ops {
		err = walEnc.Encode(op)
		if err != nil {
			return err
		}
	}
	_, err = s.database.walFile.Write(walBuf.Bytes())
	if err != nil {
		return err
	}

	// Add to buffer
	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, ops...)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALBufferSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}

// Query queries for records matching the condition
func (s *Store[T]) Query(query *Query) ([]T, error) {
	if _, ok := s.indexFields[query.Index]; !ok {
		return nil, errors.New("index not found")
	}
	valStr := ""
	if v, ok := query.Value.(string); ok {
		valStr = v
	} else {
		return nil, errors.New("value must be string")
	}

	// TODO: Should check all conditions and find the one with the least items in its index. Iterate over those items and checks for the other conditions as well.

	var results []T
	idxBucketName := string(s.bucket) + "_index_" + query.Index
	prefix := valStr + "\x00"

	err := s.database.View(func(tx *bbolt.Tx) error {
		idxB := tx.Bucket([]byte(idxBucketName))
		if idxB == nil {
			return nil
		}
		c := idxB.Cursor()
		skipped := 0
		for k, _ := c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			if skipped < query.Offset {
				skipped++
				continue
			}
			key := string(bytes.TrimPrefix(k, []byte(prefix)))
			// Get the record
			b := tx.Bucket(s.bucket)
			if b == nil {
				continue
			}
			data := b.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			err := msgpack.Unmarshal(data, &item)
			if err != nil {
				continue
			}
			results = append(results, item)
			if query.Limit > 0 && len(results) >= query.Limit {
				break
			}
		}
		return nil
	})

	return results, err
}
