package nnut

import (
	"reflect"
	"strings"
)

const (
	MaxKeyLength        = 1024
	MaxBucketNameLength = 255
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
	// Validate bucket name
	if bucketName == "" {
		return nil, BucketNameError{BucketName: bucketName, Reason: "cannot be empty"}
	}
	if len(bucketName) > MaxBucketNameLength {
		return nil, BucketNameError{BucketName: bucketName, Reason: "too long"}
	}
	for _, r := range bucketName {
		if r == '\x00' || r == '/' || r == '\\' {
			return nil, BucketNameError{BucketName: bucketName, Reason: "contains invalid character"}
		}
	}

	// Inspect struct fields at runtime to identify key and index fields for dynamic storage
	var zeroValue T
	typeOfStruct := reflect.TypeOf(zeroValue)
	if typeOfStruct.Kind() != reflect.Struct {
		return nil, InvalidTypeError{Type: typeOfStruct.String()}
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
				return nil, KeyFieldNotStringError{FieldName: field.Name}
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
		return nil, KeyFieldNotFoundError{}
	}

	// Validate index fields are strings or comparable (int)
	for indexName, fieldIndex := range indexFields {
		field := typeOfStruct.Field(fieldIndex)
		kind := field.Type.Kind()
		if kind != reflect.String && kind != reflect.Int {
			return nil, IndexFieldTypeError{FieldName: field.Name, Type: field.Type.String()}
		}
		_ = indexName // avoid unused variable
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

// validateKey checks if a key is valid
func validateKey(key string) error {
	if key == "" {
		return InvalidKeyError{Key: key}
	}
	if len(key) > MaxKeyLength {
		return InvalidKeyError{Key: key}
	}
	for _, r := range key {
		if r == '\x00' {
			return InvalidKeyError{Key: key}
		}
	}
	return nil
}
