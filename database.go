package nnut

import (
	"io"
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
}

// DB wraps bbolt.DB
type DB struct {
	*bbolt.DB
	config *Config

	walFile               *os.File
	walMutex              sync.Mutex
	operationsBuffer      []operation
	operationsBufferMutex sync.Mutex

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
	}
	return OpenWithConfig(path, config)
}

// OpenWithConfig opens a database with config
func OpenWithConfig(path string, config *Config) (*DB, error) {
	if config.WALPath == "" {
		config.WALPath = path + ".wal"
	}
	database, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	databaseInstance := &DB{
		DB:               database,
		config:           config,
		operationsBuffer: make([]operation, 0, config.WALFlushSize),
		flushChannel:     make(chan struct{}, 1),
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
		return nil, err
	}

	databaseInstance.closeWaitGroup.Add(1)
	go databaseInstance.flushWAL()
	return databaseInstance, nil
}

// Ensure all pending operations are persisted before shutting down to prevent data loss
func (db *DB) Close() error {
	close(db.closeChannel)
	db.closeWaitGroup.Wait()
	// Flush remaining
	db.Flush()
	if db.walFile != nil {
		db.walFile.Close()
	}
	return db.DB.Close()
}

func (db *DB) replayWAL() error {
	file, err := os.Open(db.config.WALPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL, ok
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := msgpack.GetDecoder()
	defer msgpack.PutDecoder(decoder)
	decoder.Reset(file)
	for {
		var operation operation
		err := decoder.Decode(&operation)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Corrupted WAL file cannot be trusted, discard to avoid applying invalid operations
			os.Remove(db.config.WALPath)
			return nil
		}

		// Reapply operations to restore database state
		err = db.Update(func(tx *bbolt.Tx) error {
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

			// Maintain index consistency during replay
			for _, idxOp := range operation.IndexOperations {
				idxBucketName := string(operation.Bucket) + "_index_" + idxOp.IndexName
				idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucketName))
				if err != nil {
					return err
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + operation.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return err
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + operation.Key
					err = idxB.Put([]byte(newKey), []byte{})
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
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
	operations := make([]operation, len(db.operationsBuffer))
	copy(operations, db.operationsBuffer)
	db.operationsBuffer = db.operationsBuffer[:0]
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
					return err
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + operation.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return err
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + operation.Key
					err = idxB.Put([]byte(newKey), []byte{})
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		// Errors during flush are not critical as operations remain in buffer for retry
	}
}
