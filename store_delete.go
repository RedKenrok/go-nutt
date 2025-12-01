package nnut

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

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
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
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
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}
