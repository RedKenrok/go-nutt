package nnut

import "context"

// Delete removes a value by key
func (s *Store[T]) Delete(ctx context.Context, key string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	// Retrieve existing value to update indexes correctly
	var oldIndexValues map[string]string
	oldValue, err := s.Get(ctx, key)
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

	return s.database.writeOperation(ctx, operation)
}

// DeleteBatch removes multiple values by keys
func (s *Store[T]) DeleteBatch(ctx context.Context, keys []string) error {
	// Fetch current values to handle index updates in batch
	oldValues, err := s.GetBatch(ctx, keys)
	if err != nil {
		return WrappedError{Operation: "get_batch", Bucket: string(s.bucket), Err: err}
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

	return s.database.writeOperations(ctx, operations)
}
