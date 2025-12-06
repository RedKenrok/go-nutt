package nnut

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestInvalidConfigError(t *testing.T) {
	err := InvalidConfigError{Field: "WALFlushSize", Value: -1, Reason: "must be positive"}
	expected := "invalid config WALFlushSize=-1: must be positive"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestInvalidQueryError(t *testing.T) {
	err := InvalidQueryError{Field: "Limit", Value: -5, Reason: "cannot be negative"}
	expected := "invalid query Limit=-5: cannot be negative"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestWALReplayError(t *testing.T) {
	underlying := errors.New("decode failed")
	err := WALReplayError{WALPath: "/tmp/test.wal", OperationIndex: 42, Err: underlying}
	expected := "WAL replay failed at operation 42 in /tmp/test.wal: decode failed"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
	if !errors.Is(err, underlying) {
		t.Error("WALReplayError should unwrap underlying error")
	}
}

func TestFlushError(t *testing.T) {
	underlying := errors.New("disk full")
	err := FlushError{OperationCount: 100, Err: underlying}
	expected := "flush failed for 100 operations: disk full"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
	if !errors.Is(err, underlying) {
		t.Error("FlushError should unwrap underlying error")
	}
}

func TestPartialBatchError(t *testing.T) {
	failed := map[string]error{
		"key1": errors.New("decode failed"),
		"key2": errors.New("not found"),
	}
	err := PartialBatchError{SuccessfulCount: 8, Failed: failed}
	expected := "batch operation partially failed: 8 successful, 2 failed"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestIndexError(t *testing.T) {
	underlying := errors.New("bucket creation failed")
	err := IndexError{IndexName: "email", Operation: "create_bucket", Bucket: "users", Key: "user123", Err: underlying}
	expected := "index 'email' create_bucket failed for bucket 'users', key 'user123': bucket creation failed"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
	if !errors.Is(err, underlying) {
		t.Error("IndexError should unwrap underlying error")
	}
}

func TestFileSystemError(t *testing.T) {
	underlying := errors.New("permission denied")
	err := FileSystemError{Path: "/tmp/test.db", Operation: "create", Err: underlying}
	expected := "filesystem operation 'create' failed for path '/tmp/test.db': permission denied"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
	if !errors.Is(err, underlying) {
		t.Error("FileSystemError should unwrap underlying error")
	}
}

func TestConcurrentAccessError(t *testing.T) {
	underlying := errors.New("buffer overflow")
	err := ConcurrentAccessError{Resource: "operations_buffer", Op: "append", Err: underlying}
	expected := "concurrent access error on operations_buffer during append: buffer overflow"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
	if !errors.Is(err, underlying) {
		t.Error("ConcurrentAccessError should unwrap underlying error")
	}
}

func TestInvalidTypeError(t *testing.T) {
	err := InvalidTypeError{Type: "int"}
	expected := "invalid type: int"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestKeyFieldNotFoundError(t *testing.T) {
	err := KeyFieldNotFoundError{}
	expected := "no field tagged with nnut:\"key\""
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestKeyFieldNotStringError(t *testing.T) {
	err := KeyFieldNotStringError{FieldName: "ID"}
	expected := "key field 'ID' must be of type string"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestInvalidFieldTypeError(t *testing.T) {
	err := InvalidFieldTypeError{FieldName: "Age", Expected: "int", Actual: "string"}
	expected := "field 'Age' has invalid type 'string', expected 'int'"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestIndexFieldTypeError(t *testing.T) {
	err := IndexFieldTypeError{FieldName: "Email", Type: "int"}
	expected := "index field 'Email' must be string, got 'int'"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestBucketNameError(t *testing.T) {
	err := BucketNameError{BucketName: "users/123", Reason: "contains invalid characters"}
	expected := "invalid bucket name 'users/123': contains invalid characters"
	if err.Error() != expected {
		t.Errorf("Expected %q, got %q", expected, err.Error())
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		wantErr  bool
		errField string
	}{
		{
			name:     "nil config",
			config:   nil,
			wantErr:  true,
			errField: "config",
		},
		{
			name: "negative WALFlushSize",
			config: &Config{
				WALFlushSize:     -1,
				WALFlushInterval: time.Minute,
				WALPath:          "/tmp/test.wal",
			},
			wantErr:  true,
			errField: "WALFlushSize",
		},
		{
			name: "zero WALFlushInterval",
			config: &Config{
				WALFlushSize:     100,
				WALFlushInterval: 0,
				WALPath:          "/tmp/test.wal",
			},
			wantErr:  true,
			errField: "WALFlushInterval",
		},
		{
			name: "empty WALPath",
			config: &Config{
				WALFlushSize:     100,
				WALFlushInterval: time.Minute,
				WALPath:          "",
			},
			wantErr:  true,
			errField: "WALPath",
		},
		{
			name: "valid config",
			config: &Config{
				WALFlushSize:     100,
				WALFlushInterval: time.Minute,
				WALPath:          "/tmp/test.wal",
				MaxBufferBytes:   1024 * 1024,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				var configErr InvalidConfigError
				if !errors.As(err, &configErr) {
					t.Errorf("Expected InvalidConfigError, got %T", err)
					return
				}
				if configErr.Field != tt.errField {
					t.Errorf("Expected field %q, got %q", tt.errField, configErr.Field)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got %v", err)
				}
			}
		})
	}
}

func TestQueryValidation(t *testing.T) {
	// Create a test store with an index
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	store, err := NewStore[TestUser](db, "users")
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	tests := []struct {
		name    string
		query   *Query
		wantErr bool
		errType interface{}
	}{
		{
			name:    "nil query",
			query:   nil,
			wantErr: true,
			errType: InvalidQueryError{},
		},
		{
			name: "negative limit",
			query: &Query{
				Limit: -1,
			},
			wantErr: true,
			errType: InvalidQueryError{},
		},
		{
			name: "negative offset",
			query: &Query{
				Offset: -5,
			},
			wantErr: true,
			errType: InvalidQueryError{},
		},
		{
			name: "non-existent index",
			query: &Query{
				Index: "nonexistent",
			},
			wantErr: true,
			errType: InvalidQueryError{},
		},
		{
			name: "valid query",
			query: &Query{
				Index:  "email", // This exists in TestUser
				Limit:  10,
				Offset: 5,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.validateQuery(tt.query)
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				if !errors.As(err, &tt.errType) {
					t.Errorf("Expected error type %T, got %T", tt.errType, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got %v", err)
				}
			}
		})
	}
}
