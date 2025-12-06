package nnut

import (
	"fmt"
)

// BucketNotFoundError indicates that the specified bucket does not exist.
type BucketNotFoundError struct {
	Bucket string
}

func (e BucketNotFoundError) Error() string {
	return fmt.Sprintf("bucket '%s' not found", e.Bucket)
}

// WrappedError wraps an underlying error with additional context.
type WrappedError struct {
	Operation     string // operation that failed
	Bucket string
	Key    string
	Err    error
}

func (e WrappedError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("%s failed for bucket '%s', key '%s': %v", e.Operation, e.Bucket, e.Key, e.Err)
	}
	return fmt.Sprintf("%s failed for bucket '%s': %v", e.Operation, e.Bucket, e.Err)
}

func (e WrappedError) Unwrap() error {
	return e.Err
}
