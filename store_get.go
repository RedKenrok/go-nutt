package nnut

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Get retrieves a value by key
func (s *Store[T]) Get(key string) (T, error) {
	var result T

	// Check buffer for pending changes first
	if op, exists := s.database.getLatestBufferedOperation(s.bucket, key); exists {
		if op.IsPut {
			// Apply buffered put operation
			decoder := msgpack.GetDecoder()
			defer msgpack.PutDecoder(decoder)
			decoder.Reset(bytes.NewReader(op.Value))
			err := decoder.Decode(&result)
			if err != nil {
				return result, WrappedError{Operation: "decode buffered", Bucket: string(s.bucket), Key: key, Err: err}
			}
			return result, nil
		} else {
			// Buffered delete operation
			return result, KeyNotFoundError{Bucket: string(s.bucket), Key: key}
		}
	}

	// No buffered changes, query database
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			return BucketNotFoundError{Bucket: string(s.bucket)}
		}
		data := bucket.Get([]byte(key))
		if data == nil {
			return KeyNotFoundError{Bucket: string(s.bucket), Key: key}
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		decoder.Reset(bytes.NewReader(data))
		err := decoder.Decode(&result)
		if err != nil {
			return WrappedError{Operation: "decode", Bucket: string(s.bucket), Key: key, Err: err}
		}
		return nil
	})
	return result, err
}

// GetBatch retrieves multiple values by keys
func (s *Store[T]) GetBatch(keys []string) (map[string]T, error) {
	results := make(map[string]T)
	failed := make(map[string]error)

	// Check buffer for pending changes first
	bufferDecoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(bufferDecoder)
	for _, key := range keys {
		if op, exists := s.database.getLatestBufferedOperation(s.bucket, key); exists {
			if op.IsPut {
				var item T
				bufferDecoder.Reset(bytes.NewReader(op.Value))
				err := bufferDecoder.Decode(&item)
				if err != nil {
					failed[key] = WrappedError{Operation: "decode buffered", Bucket: string(s.bucket), Key: key, Err: err}
					continue
				}
				results[key] = item
			}
			// For buffered deletes, don't add to results (treat as not found)
		} else {
			// No buffered change, will check DB below
		}
	}

	err := s.database.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(s.bucket)
		if bucket == nil {
			// Missing bucket indicates no data exists - return empty results
			return nil
		}
		decoder := msgpack.GetDecoder()
		defer msgpack.PutDecoder(decoder)
		for _, key := range keys {
			// Skip if already handled by buffer
			if _, alreadyHandled := results[key]; alreadyHandled {
				continue
			}
			if _, failedKey := failed[key]; failedKey {
				continue
			}

			data := bucket.Get([]byte(key))
			if data != nil {
				var item T
				decoder.Reset(bytes.NewReader(data))
				err := decoder.Decode(&item)
				if err != nil {
					// Collect decoding errors for individual items in batch
					failed[key] = WrappedError{Operation: "decode", Bucket: string(s.bucket), Key: key, Err: err}
					continue
				}
				results[key] = item
			} else {
				// Key not found - this is not an error, just missing data
				// Don't add to failed map for missing keys
			}
		}
		return nil
	})

	// Only return partial error if there were actual errors (not just missing keys)
	if err == nil && len(failed) > 0 {
		return results, PartialBatchError{SuccessfulCount: len(results), Failed: failed}
	}

	return results, err
}
