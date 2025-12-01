package nnut

import (
	"errors"
	"reflect"
	"strings"
)

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
