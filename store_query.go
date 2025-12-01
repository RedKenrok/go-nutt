package nnut

import (
	"bytes"
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

// Query queries for records matching the conditions
func (s *Store[T]) Query(query *Query) ([]T, error) {
	var results []T
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Calculate max keys to collect
		maxKeys := 0
		if query.Limit > 0 {
			maxKeys = query.Offset + query.Limit
		}

		// Collect candidate keys from conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, maxKeys)
		} else if query.Index != "" {
			// No conditions, but index for sorting, iterate index
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort)
		} else {
			// No conditions, no index, get all keys
			candidateKeys = s.getAllKeysTx(tx)
		}

		// Apply offset and limit to keys
		start := query.Offset
		if start > len(candidateKeys) {
			start = len(candidateKeys)
		}
		end := len(candidateKeys)
		if query.Limit > 0 && start+query.Limit < end {
			end = start + query.Limit
		}
		keysToFetch := candidateKeys[start:end]

		// Fetch records
		b := tx.Bucket(s.bucket)
		if b == nil {
			return nil
		}
		for _, key := range keysToFetch {
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
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Sort if Index specified and not already sorted
	if query.Index != "" && len(query.Conditions) > 0 {
		s.sortResults(results, query.Index, query.Sort)
	}

	return results, nil
}

// QueryCount returns the number of records matching the query
func (s *Store[T]) QueryCount(query *Query) (int, error) {
	var count int
	err := s.database.View(func(tx *bbolt.Tx) error {
		// Collect candidate keys from conditions
		var candidateKeys []string
		if len(query.Conditions) > 0 {
			candidateKeys = s.getCandidateKeysTx(tx, query.Conditions, 0)
		} else if query.Index != "" {
			// No conditions, but index, get all from index
			candidateKeys = s.getKeysFromIndexTx(tx, query.Index, query.Sort)
		} else {
			// No conditions, no index, get all keys
			candidateKeys = s.getAllKeysTx(tx)
		}
		count = len(candidateKeys)
		return nil
	})
	return count, err
}

// getCandidateKeysTx returns keys that match all conditions using the provided tx
func (s *Store[T]) getCandidateKeysTx(tx *bbolt.Tx, conditions []Condition, maxKeys int) []string {
	if len(conditions) == 0 {
		return s.getAllKeysTx(tx)
	}

	// Separate indexed and non-indexed conditions
	var indexedConds []Condition
	var nonIndexedConds []Condition
	for _, cond := range conditions {
		if _, ok := s.indexFields[cond.Field]; ok && cond.Value != nil {
			if _, isString := cond.Value.(string); isString {
				indexedConds = append(indexedConds, cond)
			} else {
				nonIndexedConds = append(nonIndexedConds, cond)
			}
		} else {
			nonIndexedConds = append(nonIndexedConds, cond)
		}
	}

	// Get keys from non-indexed conditions via single scan
	var nonIndexedKeys []string
	if len(nonIndexedConds) > 0 {
		nonIndexedKeys = s.scanForConditionsTx(tx, nonIndexedConds)
	}

	// Get key sets from indexed conditions
	var indexedKeySets [][]string
	for _, cond := range indexedConds {
		keysMax := 0
		if len(indexedConds) == 1 && len(nonIndexedConds) == 0 {
			keysMax = maxKeys
		}
		keys := s.getKeysForConditionTx(tx, cond, keysMax)
		indexedKeySets = append(indexedKeySets, keys)
	}

	// Intersect indexed key sets
	indexedKeys := intersectKeySlices(indexedKeySets)

	// Intersect with non-indexed
	if len(nonIndexedConds) == 0 {
		return indexedKeys
	}
	if len(indexedConds) == 0 {
		return nonIndexedKeys
	}
	// Intersect the two
	indexedMap := make(map[string]bool, len(indexedKeys))
	for _, k := range indexedKeys {
		indexedMap[k] = true
	}
	var result []string
	for _, k := range nonIndexedKeys {
		if indexedMap[k] {
			result = append(result, k)
		}
	}
	return result
}

// getKeysForConditionTx returns keys that match the condition, sorted
func (s *Store[T]) getKeysForConditionTx(tx *bbolt.Tx, cond Condition, maxKeys int) []string {
	var keys []string
	_, indexed := s.indexFields[cond.Field]
	valStr, isString := cond.Value.(string)
	if !indexed || !isString {
		// This should not happen, as we separate indexed and non-indexed
		return keys
	}

	// Use index
	idxBucketName := string(s.bucket) + "_index_" + cond.Field
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return keys
	}
	c := idxB.Cursor()
	var k []byte
	switch cond.Operator {
	case Equals:
		prefix := valStr + "\x00"
		for k, _ = c.Seek([]byte(prefix)); k != nil && bytes.HasPrefix(k, []byte(prefix)); k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	case GreaterThan:
		k, _ = c.Seek([]byte(valStr + "\x00"))
		// Skip equals
		for k != nil && bytes.HasPrefix(k, []byte(valStr+"\x00")) {
			k, _ = c.Next()
		}
		// Now collect greater
		for k != nil {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val > valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
			k, _ = c.Next()
		}
	case GreaterThanOrEqual:
		for k, _ = c.Seek([]byte(valStr + "\x00")); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val >= valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThan:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val < valStr {
					key := string(parts[1])
					keys = append(keys, key)
				} else {
					break
				}
			}
		}
	case LessThanOrEqual:
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			if maxKeys > 0 && len(keys) >= maxKeys {
				break
			}
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				val := string(parts[0])
				if val <= valStr {
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

// matchesCondition checks if the item matches the condition
func (s *Store[T]) matchesCondition(item T, cond Condition) bool {
	v := reflect.ValueOf(item)
	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Name == cond.Field {
			fieldVal := v.Field(i)
			switch cond.Operator {
			case Equals:
				return reflect.DeepEqual(fieldVal.Interface(), cond.Value)
			case GreaterThan:
				return compare(fieldVal.Interface(), cond.Value) > 0
			case LessThan:
				return compare(fieldVal.Interface(), cond.Value) < 0
			case GreaterThanOrEqual:
				return compare(fieldVal.Interface(), cond.Value) >= 0
			case LessThanOrEqual:
				return compare(fieldVal.Interface(), cond.Value) <= 0
			}
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

// getAllKeysTx returns all keys in the bucket, sorted
func (s *Store[T]) getAllKeysTx(tx *bbolt.Tx) []string {
	var keys []string
	b := tx.Bucket(s.bucket)
	if b == nil {
		return keys
	}
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		keys = append(keys, string(k))
	}
	return keys
}

// getKeysFromIndexTx returns all keys sorted by the index
func (s *Store[T]) getKeysFromIndexTx(tx *bbolt.Tx, index string, sorting Sorting) []string {
	var keys []string
	idxBucketName := string(s.bucket) + "_index_" + index
	idxB := tx.Bucket([]byte(idxBucketName))
	if idxB == nil {
		return keys
	}
	c := idxB.Cursor()
	var k []byte
	if sorting == Descending {
		for k, _ = c.Last(); k != nil; k, _ = c.Prev() {
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	} else {
		for k, _ = c.First(); k != nil; k, _ = c.Next() {
			parts := bytes.SplitN(k, []byte("\x00"), 2)
			if len(parts) == 2 {
				key := string(parts[1])
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// scanForConditionsTx scans all records and returns keys matching all conditions
func (s *Store[T]) scanForConditionsTx(tx *bbolt.Tx, conditions []Condition) []string {
	var keys []string
	b := tx.Bucket(s.bucket)
	if b == nil {
		return keys
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var item T
		err := msgpack.Unmarshal(v, &item)
		if err != nil {
			continue
		}
		matches := true
		for _, cond := range conditions {
			if !s.matchesCondition(item, cond) {
				matches = false
				break
			}
		}
		if matches {
			keys = append(keys, string(k))
		}
	}
	return keys
}

// intersectKeySlices intersects multiple key slices
func intersectKeySlices(slices [][]string) []string {
	if len(slices) == 0 {
		return []string{}
	}
	if len(slices) == 1 {
		return slices[0]
	}
	// Use maps for intersection
	sets := make([]map[string]bool, len(slices))
	for i, slice := range slices {
		sets[i] = make(map[string]bool, len(slice))
		for _, k := range slice {
			sets[i][k] = true
		}
	}
	result := make(map[string]bool)
	for key := range sets[0] {
		inAll := true
		for _, set := range sets[1:] {
			if !set[key] {
				inAll = false
				break
			}
		}
		if inAll {
			result[key] = true
		}
	}
	var res []string
	for k := range result {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// sortResults sorts the results by the index field
func (s *Store[T]) sortResults(results []T, index string, sorting Sorting) {
	fieldIdx, ok := s.indexFields[index]
	if !ok {
		return
	}
	sort.Slice(results, func(i, j int) bool {
		va := reflect.ValueOf(results[i]).Field(fieldIdx)
		vb := reflect.ValueOf(results[j]).Field(fieldIdx)
		cmp := compare(va.Interface(), vb.Interface())
		if sorting == Descending {
			return cmp > 0
		}
		return cmp < 0
	})
}
