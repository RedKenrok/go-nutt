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
	fieldMap    map[string]int // field name -> field index
}

// NewStore creates a new store for type T with the given bucket name
func NewStore[T any](database *DB, bucketName string) (*Store[T], error) {
	// Inspect struct fields at runtime to identify key and index fields for dynamic storage
	var zeroValue T
	typeOfStruct := reflect.TypeOf(zeroValue)
	if typeOfStruct.Kind() != reflect.Struct {
		return nil, errors.New("type must be a struct")
	}
	keyFieldIndex := -1
	indexFields := make(map[string]int)
	fieldMap := make(map[string]int)
	for fieldIndex := 0; fieldIndex < typeOfStruct.NumField(); fieldIndex++ {
		field := typeOfStruct.Field(fieldIndex)
		fieldMap[field.Name] = fieldIndex
		tagValue := field.Tag.Get("nnut")
		if tagValue == "key" {
			if field.Type.Kind() != reflect.String {
				return nil, errors.New("key field must be string")
			}
			keyFieldIndex = fieldIndex
		} else if strings.HasPrefix(tagValue, "index:") {
			parts := strings.Split(tagValue, ":")
			if len(parts) == 2 {
				indexName := parts[1]
				indexFields[indexName] = fieldIndex
			}
		}
	}
	if keyFieldIndex == -1 {
		return nil, errors.New("no field tagged with nnut:\"key\"")
	}
	return &Store[T]{
		database:    database,
		bucket:      []byte(bucketName),
		keyField:    keyFieldIndex,
		indexFields: indexFields,
		fieldMap:    fieldMap,
	}, nil
}

// Gather index field values to maintain secondary index consistency
func (s *Store[T]) extractIndexValues(value T) map[string]string {
	structValue := reflect.ValueOf(value)
	result := make(map[string]string)
	for indexName, fieldIndex := range s.indexFields {
		fieldValue := structValue.Field(fieldIndex)
		if fieldValue.Kind() == reflect.String {
			result[indexName] = fieldValue.String()
		}
	}
	return result
}
