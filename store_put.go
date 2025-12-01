package nnut

import (
	"bytes"
	"errors"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

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
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
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

		buf := bufferPool.Get().(*bytes.Buffer)
		defer bufferPool.Put(buf)
		buf.Reset()
		enc := msgpack.NewEncoder(buf)
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
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}
