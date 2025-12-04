package nnut

import (
	"bytes"
	"errors"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Get retrieves a value by key
func (s *Store[T]) Get(key string) (T, error) {
	var result T
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return errors.New("bucket not found")
		}
		data := bucket.Get([]byte(key))
		if data == nil {
			return errors.New("key not found")
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		decoder.Reset(bytes.NewReader(data))
		return decoder.Decode(&result)
	})
	return result, err
}

// GetBatch retrieves multiple values by keys
func (s *Store[T]) GetBatch(keys []string) (map[string]T, error) {
	results := make(map[string]T)
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			// Missing bucket indicates no data exists
			return nil
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range keys {
			data := bucket.Get([]byte(key))
			if data != nil {
				var item T
				decoder.Reset(bytes.NewReader(data))
				err := decoder.Decode(&item)
				if err != nil {
					continue
				}
				results[key] = item
			}
		}
		return nil
	})
	return results, err
}
