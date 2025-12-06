package nnut

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

var mapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]bool)
	},
}

type Operator int

const (
	Equals Operator = iota
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
)

type Sorting int

const (
	Unsorted Sorting = iota
	Ascending
	Descending
)

type Condition struct {
	Field    string
	Value    interface{}
	Operator Operator
}

type Query struct {
	Index  string
	Limit  int // Maximum number of results to return (0 = no limit)
	Offset int // Number of results to skip
	Sort   Sorting

	Conditions []Condition
}

// validateQuery validates query parameters
func (s *Store[T]) validateQuery(query *Query) error {
	if query == nil {
		return InvalidQueryError{Field: "query", Value: nil, Reason: "cannot be nil"}
	}
	if query.Limit < 0 {
		return InvalidQueryError{Field: "Limit", Value: query.Limit, Reason: "cannot be negative"}
	}
	if query.Offset < 0 {
		return InvalidQueryError{Field: "Offset", Value: query.Offset, Reason: "cannot be negative"}
	}
	if query.Index != "" {
		if _, exists := s.indexFields[query.Index]; !exists {
			return InvalidQueryError{Field: "Index", Value: query.Index, Reason: "index field does not exist"}
		}
	}
	// Validate conditions
	for _, cond := range query.Conditions {
		if _, exists := s.fieldMap[cond.Field]; !exists {
			return InvalidQueryError{Field: "Condition.Field", Value: cond.Field, Reason: "field does not exist"}
		}
		// Check if value is comparable (string or int)
		if cond.Value != nil {
			switch cond.Value.(type) {
			case string, int:
				// ok
			default:
				return InvalidQueryError{Field: "Condition.Value", Value: cond.Value, Reason: "must be string or int"}
			}
		}
	}
	return nil
}

type condWithSize struct {
	cond Condition
	size int
}

// Query queries for records matching the conditions
func (s *Store[T]) Query(ctx context.Context, query *Query) ([]T, error) {
	if err := s.validateQuery(query); err != nil {
		return nil, err
	}

	var results []T
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Determine the maximum number of keys needed based on limit and offset
		maxKeys := 0
		if query.Limit > 0 {
			maxKeys = query.Offset + query.Limit
		}

		// Gather keys that potentially match the query conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, maxKeys)
		} else if query.Index != "" {
			// When no conditions but sorting is required, use the index directly
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort, maxKeys)
		} else {
			// Fallback to scanning all keys when no optimizations apply
			candidateKeys = s.getAllKeysTx(tx, maxKeys)
		}

		// Skip offset and take only limit number of keys
		start := query.Offset
		if start > len(candidateKeys) {
			start = len(candidateKeys)
		}
		end := len(candidateKeys)
		if query.Limit > 0 && start+query.Limit < end {
			end = start + query.Limit
		}
		keysToFetch := candidateKeys[start:end]

		// Retrieve the actual data for the selected keys
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range keysToFetch {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			if err != nil {
				continue
			}
			results = append(results, item)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Apply sorting if the index wasn't used for ordering
	if query.Index != "" && len(query.Conditions) > 0 {
		s.sortResults(results, query.Index, query.Sort)
	}

	return results, nil
}

// QueryCount returns the number of records matching the query
func (s *Store[T]) QueryCount(ctx context.Context, query *Query) (int, error) {
	if err := s.validateQuery(query); err != nil {
		return 0, err
	}

	var count int
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Collect candidate keys from conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, 0)
		} else if query.Index != "" {
			// No conditions, but index, count from index
			count = s.countKeysFromIndexTx(tx, query.Index)
			return nil
		} else {
			// No conditions, no index, count all keys
			count = s.countAllKeysTx(tx)
			return nil
		}
		count = len(candidateKeys)
		return nil
	})
	return count, err
}

// getCandidateKeysTx returns keys that match all conditions using the provided tx
func (s *Store[T]) getCandidateKeysTx(tx *bbolt.Tx, conditions []Condition, maxKeys int) []string {
	if len(conditions) == 0 {
		return s.getAllKeysTx(tx, maxKeys)
	}

	// Partition conditions to leverage indexes where possible
	var indexedConditions []Condition
	var nonIndexedConditions []Condition
	for _, condition := range conditions {
		if _, ok := s.indexFields[condition.Field]; ok && condition.Value != nil {
			if _, isString := condition.Value.(string); isString {
				indexedConditions = append(indexedConditions, condition)
			} else {
				nonIndexedConditions = append(nonIndexedConditions, condition)
			}
		} else {
			nonIndexedConditions = append(nonIndexedConditions, condition)
		}
	}

	// Get key sets from indexed conditions, starting with the shortest
	var indexedKeys []string
	if len(indexedConditions) > 0 {
		var conditionSizes []condWithSize
		for _, condition := range indexedConditions {
			size := s.countKeysForConditionTx(tx, condition, maxKeys)
			conditionSizes = append(conditionSizes, condWithSize{condition, size})
		}
		// Sort by size ascending
		sort.Slice(conditionSizes, func(i, j int) bool {
			return conditionSizes[i].size < conditionSizes[j].size
		})
		// Primary is the smallest
		primaryCondition := conditionSizes[0].cond
		keysMax := 0
		if len(indexedConditions) == 1 && len(nonIndexedConditions) == 0 {
			keysMax = maxKeys
		}
		indexedKeys = s.getKeysForConditionTx(tx, primaryCondition, keysMax)
		// Intersect others into primary
		for index := 1; index < len(conditionSizes); index++ {
			otherConditionKeys := s.getKeysForConditionTx(tx, conditionSizes[index].cond, 0)
			indexedKeys = intersectSlices(indexedKeys, otherConditionKeys)
		}
	}

	// Get keys from non-indexed conditions via single scan
	var nonIndexedKeys []string
	if len(nonIndexedConditions) > 0 {
		// If we have indexed keys, scan only those; otherwise scan all
		var candidates []string
		if len(indexedConditions) > 0 {
			candidates = indexedKeys
		}
		nonIndexedKeys = s.scanForConditionsTx(tx, nonIndexedConditions, candidates, maxKeys)
	}

	// Intersect with non-indexed
	if len(nonIndexedConditions) == 0 {
		return indexedKeys
	}
	if len(indexedConditions) == 0 {
		return nonIndexedKeys
	}
	// Intersect the two
	indexedMap := make(map[string]bool, len(indexedKeys))
	for _, key := range indexedKeys {
		indexedMap[key] = true
	}
	var result []string
	for _, key := range nonIndexedKeys {
		if indexedMap[key] {
			result = append(result, key)
		}
	}
	return result
}

// getKeysForConditionTx returns keys that match the condition, sorted
func (s *Store[T]) getKeysForConditionTx(tx *bbolt.Tx, condition Condition, maxKeys int) []string {
	var keys []string
	_, indexed := s.indexFields[condition.Field]
	valueString, isString := condition.Value.(string)
	if !indexed || !isString {
		// This should not happen, as we separate indexed and non-indexed
		return keys
	}

	// Use index
	indexBucketName := string(s.bucket) + "_index_" + condition.Field
	indexBucket := tx.Bucket([]byte(indexBucketName))
	if indexBucket == nil {
		return keys
	}
	cursor := indexBucket.Cursor()
	var keyBytes []byte
	switch condition.Operator {
	case Equals:
		prefix := valueString + "\x00"
		for keyBytes, _ = cursor.Seek([]byte(prefix)); keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(prefix)); keyBytes, _ = cursor.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	case GreaterThan:
		keyBytes, _ = cursor.Seek([]byte(valueString + "\x00"))
		// Skip equals
		for keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(valueString+"\x00")) {
			keyBytes, _ = cursor.Next()
		}
		// Now collect greater
		for keyBytes != nil {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value > valueString {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
			keyBytes, _ = cursor.Next()
		}
	case GreaterThanOrEqual:
		for keyBytes, _ = cursor.Seek([]byte(valueString + "\x00")); keyBytes != nil; keyBytes, _ = cursor.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value >= valueString {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThan:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value < valueString {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThanOrEqual:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value <= valueString {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// countKeysForConditionTx returns the count of keys matching the condition
func (s *Store[T]) countKeysForConditionTx(tx *bbolt.Tx, condition Condition, maxKeys int) int {
	var count int
	_, indexed := s.indexFields[condition.Field]
	valueString, isString := condition.Value.(string)
	if !indexed || !isString {
		return 0
	}

	indexBucketName := string(s.bucket) + "_index_" + condition.Field
	indexBucket := tx.Bucket([]byte(indexBucketName))
	if indexBucket == nil {
		return 0
	}
	cursor := indexBucket.Cursor()
	var keyBytes []byte
	switch condition.Operator {
	case Equals:
		prefix := valueString + "\x00"
		for keyBytes, _ = cursor.Seek([]byte(prefix)); keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(prefix)); keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
		}
	case GreaterThan:
		keyBytes, _ = cursor.Seek([]byte(valueString + "\x00"))
		// Skip equals
		for keyBytes != nil && bytes.HasPrefix(keyBytes, []byte(valueString+"\x00")) {
			keyBytes, _ = cursor.Next()
		}
		// Now count greater
		for keyBytes != nil {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value <= valueString {
					break
				}
			}
			keyBytes, _ = cursor.Next()
		}
	case GreaterThanOrEqual:
		for keyBytes, _ = cursor.Seek([]byte(valueString + "\x00")); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value < valueString {
					break
				}
			}
		}
	case LessThan:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value >= valueString {
					break
				}
			}
		}
	case LessThanOrEqual:
		for keyBytes, _ = cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
			count++
			if maxKeys > 0 && count >= maxKeys {
				break
			}
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				value := string(parts[0])
				if value > valueString {
					break
				}
			}
		}
	}
	return count
}

// matchesCondition checks if the item matches the condition
func (s *Store[T]) matchesCondition(item T, condition Condition) bool {
	itemValue := reflect.ValueOf(item)
	if fieldIndex, ok := s.fieldMap[condition.Field]; ok {
		fieldValue := itemValue.Field(fieldIndex)
		switch condition.Operator {
		case Equals:
			return reflect.DeepEqual(fieldValue.Interface(), condition.Value)
		case GreaterThan:
			return compare(fieldValue.Interface(), condition.Value) > 0
		case LessThan:
			return compare(fieldValue.Interface(), condition.Value) < 0
		case GreaterThanOrEqual:
			return compare(fieldValue.Interface(), condition.Value) >= 0
		case LessThanOrEqual:
			return compare(fieldValue.Interface(), condition.Value) <= 0
		}
	}
	return false
}

// compare compares two values, assumes comparable types
func compare(a, b interface{}) int {
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0 // not comparable, treat as equal
}

// getAllKeysTx returns all keys in the bucket, sorted, up to maxKeys if >0
func (s *Store[T]) getAllKeysTx(tx *bbolt.Tx, maxKeys int) []string {
	var keys []string
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return empty
		return keys
	}
	cursor := bucket.Cursor()
	for keyBytes, _ := cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
		if maxKeys > 0 && len(keys) >= maxKeys {
			break
		}
		keys = append(keys, string(keyBytes))
	}
	return keys
}

// countAllKeysTx returns the count of all keys in the bucket
func (s *Store[T]) countAllKeysTx(tx *bbolt.Tx) int {
	count := 0
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return 0
		return count
	}
	cursor := bucket.Cursor()
	for keyBytes, _ := cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
		count++
	}
	return count
}

// getKeysFromIndexTx returns all keys sorted by the index
func (s *Store[T]) getKeysFromIndexTx(tx *bbolt.Tx, index string, sorting Sorting, maxKeys int) []string {
	var keys []string
	indexBucketName := string(s.bucket) + "_index_" + index
	indexBucket := tx.Bucket([]byte(indexBucketName))
	if indexBucket == nil {
		return keys
	}
	cursor := indexBucket.Cursor()
	var keyBytes []byte
	if sorting == Descending {
		for keyBytes, _ = cursor.Last(); keyBytes != nil && (maxKeys == 0 || len(keys) < maxKeys); keyBytes, _ = cursor.Prev() {
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	} else {
		for keyBytes, _ = cursor.First(); keyBytes != nil && (maxKeys == 0 || len(keys) < maxKeys); keyBytes, _ = cursor.Next() {
			parts := bytes.SplitN(keyBytes, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// countKeysFromIndexTx returns the count of keys in the index
func (s *Store[T]) countKeysFromIndexTx(tx *bbolt.Tx, index string) int {
	count := 0
	indexBucketName := string(s.bucket) + "_index_" + index
	indexBucket := tx.Bucket([]byte(indexBucketName))
	if indexBucket == nil {
		return count
	}
	cursor := indexBucket.Cursor()
	for keyBytes, _ := cursor.First(); keyBytes != nil; keyBytes, _ = cursor.Next() {
		count++
	}
	return count
}

// scanForConditionsTx scans records and returns keys matching all conditions
// If candidates is not nil, only scans those keys; otherwise scans all.
// Limits to maxKeys if >0.
func (s *Store[T]) scanForConditionsTx(tx *bbolt.Tx, conditions []Condition, candidates []string, maxKeys int) []string {
	var keys []string
	bucket := tx.Bucket(s.bucket)
	if bucket == nil {
		// Bucket not found, return empty
		return keys
	}
	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	if candidates != nil {
		// Scan only candidate keys
		for _, key := range candidates {
			data := bucket.Get([]byte(key))
			if data == nil {
				continue
			}
			var item T
			decoder.Reset(bytes.NewReader(data))
			err := decoder.Decode(&item)
			if err != nil {
				continue
			}
			matches := true
			for _, condition := range conditions {
				if !s.matchesCondition(item, condition) {
					matches = false
					break
				}
			}
			if matches {
				keys = append(keys, key)
				if maxKeys > 0 && len(keys) >= maxKeys {
					break
				}
			}
		}
	} else {
		// Scan all records
		cursor := bucket.Cursor()
		for keyBytes, valueBytes := cursor.First(); keyBytes != nil; keyBytes, valueBytes = cursor.Next() {
			var item T
			decoder.Reset(bytes.NewReader(valueBytes))
			err := decoder.Decode(&item)
			if err != nil {
				continue
			}
			matches := true
			for _, condition := range conditions {
				if !s.matchesCondition(item, condition) {
					matches = false
					break
				}
			}
			if matches {
				keys = append(keys, string(keyBytes))
				if maxKeys > 0 && len(keys) >= maxKeys {
					break
				}
			}
		}
	}
	return keys
}

// intersectSlices intersects two key slices, returning keys in base that are also in other
func intersectSlices(base, other []string) []string {
	baseMap := make(map[string]bool, len(base))
	for _, k := range base {
		baseMap[k] = true
	}
	var result []string
	for _, k := range other {
		if baseMap[k] {
			result = append(result, k)
		}
	}
	return result
}

// sortResults sorts the results by the index field
func (s *Store[T]) sortResults(results []T, index string, sorting Sorting) {
	fieldIndex, ok := s.indexFields[index]
	if !ok {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		valueA := reflect.ValueOf(results[i]).Field(fieldIndex)
		valueB := reflect.ValueOf(results[j]).Field(fieldIndex)
		comparison := compare(valueA.Interface(), valueB.Interface())
		if sorting == Descending {
			return comparison > 0
		}
		return comparison < 0
	})
}
