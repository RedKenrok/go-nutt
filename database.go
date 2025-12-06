package nnut

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"go.etcd.io/bbolt"
)

// Config holds configuration options
type Config struct {
	WALFlushSize     int
	WALFlushInterval time.Duration
	WALPath          string
	MaxBufferBytes   int
	FlushChannelSize int // Size of the flush channel buffer (default 10)
}

// DB wraps bbolt.DB
type DB struct {
	*bbolt.DB
	config *Config

	walFile               *os.File
	walMutex              sync.Mutex
	operationsBuffer      map[string]operation
	operationsBufferMutex sync.Mutex
	bytesInBuffer         uint64

	flushChannel   chan struct{}
	closeChannel   chan struct{}
	closeWaitGroup sync.WaitGroup
}

type indexOperation struct {
	IndexName string
	OldValue  string
	NewValue  string
}

type operation struct {
	Bucket          []byte
	Key             string
	Value           []byte
	IsPut           bool
	IndexOperations []indexOperation
}

// Open opens a database with default config
func Open(path string) (*DB, error) {
	config := &Config{
		WALFlushSize:     1024,
		WALFlushInterval: time.Minute * 15,
		WALPath:          path + ".wal",
		MaxBufferBytes:   10 * 1024 * 1024, // 10MB
		FlushChannelSize: 10,
	}
	return OpenWithConfig(path, config)
}

// validateConfig validates the configuration parameters
func validateConfig(config *Config) error {
	if config == nil {
		return InvalidConfigError{Field: "config", Value: nil, Reason: "cannot be nil"}
	}
	if config.WALFlushSize <= 0 {
		return InvalidConfigError{Field: "WALFlushSize", Value: config.WALFlushSize, Reason: "must be positive"}
	}
	if config.WALFlushInterval <= 0 {
		return InvalidConfigError{Field: "WALFlushInterval", Value: config.WALFlushInterval, Reason: "must be positive"}
	}
	if config.WALPath == "" {
		return InvalidConfigError{Field: "WALPath", Value: config.WALPath, Reason: "cannot be empty"}
	}
	if config.MaxBufferBytes <= 0 {
		return InvalidConfigError{Field: "MaxBufferBytes", Value: config.MaxBufferBytes, Reason: "must be positive"}
	}
	if config.FlushChannelSize < 0 {
		return InvalidConfigError{Field: "FlushChannelSize", Value: config.FlushChannelSize, Reason: "cannot be negative"}
	}
	return nil
}

// OpenWithConfig opens a database with config
func OpenWithConfig(path string, config *Config) (*DB, error) {
	if config != nil && config.WALPath == "" {
		config.WALPath = path + ".wal"
	}
	if config != nil && config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 10 * 1024 * 1024 // 10MB
	}
	if config != nil && config.FlushChannelSize == 0 {
		config.FlushChannelSize = 10
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if config != nil && config.WALPath == "" {
		config.WALPath = path + ".wal"
	}
	if config != nil && config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 10 * 1024 * 1024 // 10MB
	}

	if err := validateConfig(config); err != nil {
		return nil, err
	}
	database, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, FileSystemError{Path: path, Operation: "open", Err: err}
	}
	databaseInstance := &DB{
		DB:               database,
		config:           config,
		operationsBuffer: make(map[string]operation),
		flushChannel:     make(chan struct{}, config.FlushChannelSize),
		closeChannel:     make(chan struct{}),
	}

	// Recover uncommitted operations from previous session to ensure data consistency
	err = databaseInstance.replayWAL()
	if err != nil {
		database.Close()
		return nil, err
	}

	// Prepare WAL file for logging new operations to enable crash recovery
	databaseInstance.walFile, err = os.OpenFile(config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		database.Close()
		return nil, FileSystemError{Path: config.WALPath, Operation: "create", Err: err}
	}

	databaseInstance.closeWaitGroup.Add(1)
	go databaseInstance.flushWAL()
	return databaseInstance, nil
}

// getLatestBufferedOperation checks the buffer for pending changes to a key
func (db *DB) getLatestBufferedOperation(bucket []byte, key string) (operation, bool) {
	db.operationsBufferMutex.Lock()
	defer db.operationsBufferMutex.Unlock()
	op, exists := db.operationsBuffer[bufferKey(bucket, key)]
	return op, exists
}

func (db *DB) replayWAL() error {
	file, err := os.Open(db.config.WALPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL, ok
			return nil
		}
		return FileSystemError{Path: db.config.WALPath, Operation: "open", Err: err}
	}
	defer file.Close()

	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	decoder.Reset(file)
	operationIndex := 0
	for {
		var operation operation
		err := decoder.Decode(&operation)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Corrupted WAL file cannot be trusted, discard to avoid applying invalid operations
			os.Remove(db.config.WALPath)
			return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
		}

		// Reapply operations to restore database state
		err = db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(operation.Bucket)
			if err != nil {
				return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
			}
			if operation.IsPut {
				err = b.Put([]byte(operation.Key), operation.Value)
				if err != nil {
					return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
				}
			} else {
				err = b.Delete([]byte(operation.Key))
				if err != nil {
					return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: err}
				}
			}

			// Maintain index consistency during replay
			for _, idxOp := range operation.IndexOperations {
				idxBucketName := string(operation.Bucket) + "_index_" + idxOp.IndexName
				idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucketName))
				if err != nil {
					return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: IndexError{IndexName: idxOp.IndexName, Operation: "create_bucket", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}}
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + operation.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: IndexError{IndexName: idxOp.IndexName, Operation: "delete", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}}
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + operation.Key
					err = idxB.Put([]byte(newKey), []byte{})
					if err != nil {
						return WALReplayError{WALPath: db.config.WALPath, OperationIndex: operationIndex, Err: IndexError{IndexName: idxOp.IndexName, Operation: "put", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}}
					}
				}
			}
			return nil
		})
		if err != nil {
			return WrappedError{Operation: "replay_wal", Err: err}
		}
		operationIndex++
	}

	// WAL is no longer needed after successful replay
	os.Remove(db.config.WALPath)

	return nil
}

func (db *DB) flushWAL() {
	defer db.closeWaitGroup.Done()
	ticker := time.NewTicker(db.config.WALFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.Flush()
		case <-db.flushChannel:
			db.Flush()
			ticker.Reset(db.config.WALFlushInterval)
		case <-db.closeChannel:
			return
		}
	}
}

func (db *DB) Flush() {
	db.operationsBufferMutex.Lock()
	operations := make([]operation, 0, len(db.operationsBuffer))
	for _, op := range db.operationsBuffer {
		operations = append(operations, op)
	}
	db.operationsBuffer = make(map[string]operation)
	db.bytesInBuffer = 0
	db.operationsBufferMutex.Unlock()

	if len(operations) == 0 {
		return
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		for _, operation := range operations {
			b, err := tx.CreateBucketIfNotExists(operation.Bucket)
			if err != nil {
				return err
			}
			if operation.IsPut {
				err = b.Put([]byte(operation.Key), operation.Value)
				if err != nil {
					return err
				}
			} else {
				err = b.Delete([]byte(operation.Key))
				if err != nil {
					return err
				}
			}
			for _, idxOp := range operation.IndexOperations {
				idxBucketName := string(operation.Bucket) + "_index_" + idxOp.IndexName
				idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucketName))
				if err != nil {
					return IndexError{IndexName: idxOp.IndexName, Operation: "create_bucket", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + operation.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return IndexError{IndexName: idxOp.IndexName, Operation: "delete", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + operation.Key
					err = idxB.Put([]byte(newKey), []byte{})
					if err != nil {
						return IndexError{IndexName: idxOp.IndexName, Operation: "put", Bucket: string(operation.Bucket), Key: operation.Key, Err: err}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		// Log flush errors for debugging, but don't fail the operation as operations remain in buffer for retry
		log.Printf("Flush error: %v", FlushError{OperationCount: len(operations), Err: err})
	}
}

// bufferKey generates a unique key for the operations buffer
func bufferKey(bucket []byte, key string) string {
	return string(bucket) + "\x00" + key
}

// writeOperation adds a single operation to WAL and buffer
func (db *DB) writeOperation(ctx context.Context, op operation) error {
	// Encode operation to measure size
	var buf bytes.Buffer
	encoder := msgpack.NewEncoder(&buf)
	err := encoder.Encode(op)
	if err != nil {
		return WrappedError{Operation: "encode WAL", Err: err}
	}
	encodedBytes := buf.Bytes()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Write to WAL file
	db.walMutex.Lock()
	_, err = db.walFile.Write(encodedBytes)
	db.walMutex.Unlock()
	if err != nil {
		return FileSystemError{Path: db.config.WALPath, Operation: "write", Err: err}
	}

	// Add to buffer with deduplication
	db.operationsBufferMutex.Lock()
	key := bufferKey(op.Bucket, op.Key)
	db.operationsBuffer[key] = op
	db.bytesInBuffer += uint64(len(encodedBytes))
	shouldFlush := db.bytesInBuffer >= uint64(db.config.MaxBufferBytes)
	db.operationsBufferMutex.Unlock()

	if shouldFlush {
		select {
		case db.flushChannel <- struct{}{}:
		default:
		}
	}
	return nil
}

// writeOperations adds multiple operations to WAL and buffer atomically
func (db *DB) writeOperations(ctx context.Context, ops []operation) error {
	if len(ops) == 0 {
		return nil
	}

	// Encode all operations
	var walBuffer bytes.Buffer
	walEncoder := msgpack.NewEncoder(&walBuffer)
	totalBytes := uint64(0)
	for _, op := range ops {
		err := walEncoder.Encode(op)
		if err != nil {
			return WrappedError{Operation: "encode WAL batch", Err: err}
		}
		// Measure size (approximate, since we already encoded)
		var tempBuf bytes.Buffer
		tempEncoder := msgpack.NewEncoder(&tempBuf)
		tempEncoder.Encode(op)
		totalBytes += uint64(tempBuf.Len())
	}
	walBytes := walBuffer.Bytes()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Write batch to WAL file
	db.walMutex.Lock()
	_, err := db.walFile.Write(walBytes)
	db.walMutex.Unlock()
	if err != nil {
		return FileSystemError{Path: db.config.WALPath, Operation: "write_batch", Err: err}
	}

	// Add to buffer with deduplication
	db.operationsBufferMutex.Lock()
	for _, op := range ops {
		key := bufferKey(op.Bucket, op.Key)
		db.operationsBuffer[key] = op
	}
	db.bytesInBuffer += totalBytes
	shouldFlush := db.bytesInBuffer >= uint64(db.config.MaxBufferBytes)
	db.operationsBufferMutex.Unlock()

	if shouldFlush {
		select {
		case db.flushChannel <- struct{}{}:
		default:
		}
	}
	return nil
}
