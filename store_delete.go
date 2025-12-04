package nnut

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// Delete removes a value by key
func (s *Store[T]) Delete(key string) error {
	// Retrieve existing value to update indexes correctly
	var oldIndexValues map[string]string
	oldValue, err := s.Get(key)
	if err == nil {
		oldIndexValues = s.extractIndexValues(oldValue)
	} else {
		oldIndexValues = make(map[string]string)
	}

	// Set up index removals for each deleted item
	var indexOperations []indexOperation
	for name := range s.indexFields {
		oldValue := oldIndexValues[name]
		if oldValue != "" {
			indexOperations = append(indexOperations, indexOperation{
				IndexName: name,
				OldValue:  oldValue,
				NewValue:  "",
			})
		}
	}

	operation := operation{
		Bucket:          s.bucket,
		Key:             key,
		Value:           nil,
		IsPut:           false,
		IndexOperations: indexOperations,
	}

	// Log the operation for crash recovery
	s.database.walMutex.Lock()
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	err = walEncoder.Encode(operation)
	if err != nil {
		s.database.walMutex.Unlock()
		return err
	}
	_, err = s.database.walFile.Write(walBuffer.Bytes())
	s.database.walMutex.Unlock()
	if err != nil {
		return err
	}

	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, operation)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}
	return nil
}

// DeleteBatch removes multiple values by keys
func (s *Store[T]) DeleteBatch(keys []string) error {
	// Fetch current values to handle index updates in batch
	oldValues, err := s.GetBatch(keys)
	if err != nil {
		return err
	}

	// Build operations for each key to be deleted
	var operations []operation
	for _, key := range keys {
		oldValue, exists := oldValues[key]
		var oldIndexValues map[string]string
		if exists {
			oldIndexValues = s.extractIndexValues(oldValue)
		} else {
			oldIndexValues = make(map[string]string)
		}

		// Prepare index updates for deletion
		var indexOperations []indexOperation
		for name := range s.indexFields {
			oldValue := oldIndexValues[name]
			if oldValue != "" {
				indexOperations = append(indexOperations, indexOperation{
					IndexName: name,
					OldValue:  oldValue,
					NewValue:  "",
				})
			}
		}

		operation := operation{
			Bucket:          s.bucket,
			Key:             key,
			Value:           nil,
			IsPut:           false,
			IndexOperations: indexOperations,
		}
		operations = append(operations, operation)
	}

	// Write all operations to WAL atomically
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	for _, operation := range operations {
		err = walEncoder.Encode(operation)
		if err != nil {
			return err
		}
	}
	s.database.walMutex.Lock()
	_, err = s.database.walFile.Write(walBuffer.Bytes())
	s.database.walMutex.Unlock()
	if err != nil {
		return err
	}

	// Queue operations for eventual flush to disk
	s.database.operationsBufferMutex.Lock()
	s.database.operationsBuffer = append(s.database.operationsBuffer, operations...)
	shouldFlush := len(s.database.operationsBuffer) >= s.database.config.WALFlushSize
	s.database.operationsBufferMutex.Unlock()

	if shouldFlush {
		s.database.flushChannel <- struct{}{}
	}

	return nil
}
