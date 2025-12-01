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
		b := tx.Bucket(s.bucket)
		if b == nil {
			return errors.New("bucket not found")
		}
		data := b.Get([]byte(key))
		if data == nil {
			return errors.New("key not found")
		}
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		dec.Reset(bytes.NewReader(data))
		return dec.Decode(&result)
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
		dec := msgpack.GetDecoder()
		defer msgpack.PutDecoder(dec)
		for _, key := range keys {
			data := b.Get([]byte(key))
			if data != nil {
				var item T
				dec.Reset(bytes.NewReader(data))
				err := dec.Decode(&item)
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
