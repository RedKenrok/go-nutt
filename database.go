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
	WALFlushSize    int
	WALFlushInterval time.Duration
	WALPath          string
}

// DB wraps bbolt.DB
type DB struct {
	*bbolt.DB
	config *Config

	walFile               *os.File
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
		WALFlushSize:    1024,
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
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	d := &DB{
		DB:               db,
		config:           config,
		operationsBuffer: make([]operation, 0, config.WALFlushSize),
		flushChannel:     make(chan struct{}, 1),
		closeChannel:     make(chan struct{}),
	}

	// Replay WAL if exists
	err = d.replayWAL()
	if err != nil {
		db.Close()
		return nil, err
	}

	// Open WAL file for append
	d.walFile, err = os.OpenFile(config.WALPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		db.Close()
		return nil, err
	}

	d.closeWaitGroup.Add(1)
	go d.flushWAL()
	return d, nil
}

// Close closes the database and flushes the WAL
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

	dec := msgpack.NewDecoder(file)
	for {
		var op operation
		err := dec.Decode(&op)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Invalid WAL, remove
			os.Remove(db.config.WALPath)
			return nil
		}

		// Apply to database
		err = db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(op.Bucket)
			if err != nil {
				return err
			}
			if op.IsPut {
				err = b.Put([]byte(op.Key), op.Value)
				if err != nil {
					return err
				}
			} else {
				err = b.Delete([]byte(op.Key))
				if err != nil {
					return err
				}
			}

			// Update indices
			for _, idxOp := range op.IndexOperations {
				idxBucketName := string(op.Bucket) + "_index_" + idxOp.IndexName
				idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucketName))
				if err != nil {
					return err
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + op.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return err
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + op.Key
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

	// After replay, remove WAL file
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
	ops := make([]operation, len(db.operationsBuffer))
	copy(ops, db.operationsBuffer)
	db.operationsBuffer = db.operationsBuffer[:0]
	db.operationsBufferMutex.Unlock()

	if len(ops) == 0 {
		return
	}

	err := db.Update(func(tx *bbolt.Tx) error {
		for _, op := range ops {
			b, err := tx.CreateBucketIfNotExists(op.Bucket)
			if err != nil {
				return err
			}
			if op.IsPut {
				err = b.Put([]byte(op.Key), op.Value)
				if err != nil {
					return err
				}
			} else {
				err = b.Delete([]byte(op.Key))
				if err != nil {
					return err
				}
			}
			for _, idxOp := range op.IndexOperations {
				idxBucketName := string(op.Bucket) + "_index_" + idxOp.IndexName
				idxB, err := tx.CreateBucketIfNotExists([]byte(idxBucketName))
				if err != nil {
					return err
				}
				if idxOp.OldValue != "" {
					oldKey := idxOp.OldValue + "\x00" + op.Key
					err = idxB.Delete([]byte(oldKey))
					if err != nil {
						return err
					}
				}
				if idxOp.NewValue != "" {
					newKey := idxOp.NewValue + "\x00" + op.Key
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
		// log error? for now, ignore
	}
}
