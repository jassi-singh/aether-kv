// Package engine provides the core key-value storage engine implementation.
// It manages the in-memory key directory (keyDir) and coordinates with the
// storage layer for persistent data operations.
package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/format"
	"github.com/jassi-singh/aether-kv/internal/storage"
)

// Key represents a single entry in the key directory, mapping a key name
// to its location in the log file. The key directory is an in-memory index
// that provides fast lookups without scanning the entire log file.
type Key struct {
	FileId uint32 // File identifier (currently 0 for single-file implementation)
	Size   uint32 // Total size of the record (header + key + value)
	Offset int64  // Byte offset where the record starts in the log file
}

// NewKeyDir creates and returns a new empty key directory sync.Map.
// The key directory maps string keys to their file location metadata.
// sync.Map is used for thread-safe concurrent access without explicit locking.
func NewKeyDir() *sync.Map {
	return &sync.Map{}
}

// Engine defines the interface for key-value storage operations.
type Engine interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
	Close() error
	GetKeyDirSize() int
	RecoverKeyDir() error
}

// KVEngine is the main implementation of the key-value storage engine.
// It maintains an in-memory key directory (keyDir) that maps keys to their
// file locations and coordinates with the storage layer for persistence.
type KVEngine struct {
	keyDir *sync.Map       // Thread-safe in-memory index mapping keys to file locations
	file   storage.Storage // Storage interface for file operations
	cfg    *config.Config  // Configuration injected at initialization
}

// NewKVEngine creates and initializes a new KVEngine instance.
// It loads configuration, opens the storage file, and recovers the key
// directory from disk. Returns an error if initialization fails.
func NewKVEngine(cfg *config.Config) (*KVEngine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	slog.Info("engine: initializing KV engine")

	file, err := storage.NewFile(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create file handler: %w", err)
	}

	engine := &KVEngine{
		keyDir: NewKeyDir(),
		file:   file,
		cfg:    cfg,
	}

	if err := engine.RecoverKeyDir(); err != nil {
		return nil, fmt.Errorf("failed to recover keyDir: %w", err)
	}

	slog.Info("engine: KV engine initialized successfully")
	return engine, nil
}

// Get retrieves the value associated with the given key.
// It first checks the in-memory key directory, then reads the record from disk.
// Returns an error if the key is not found or if any I/O operation fails.
func (e *KVEngine) Get(key string) (string, error) {
	value, ok := e.keyDir.Load(key)
	if !ok {
		slog.Debug("get: key not found in keyDir",
			"key", key)
		return "", errors.New("key not found")
	}

	keyEntry, ok := value.(*Key)
	if !ok {
		return "", fmt.Errorf("invalid key entry type for key %s", key)
	}

	slog.Debug("get: reading record from file",
		"key", key,
		"offset", keyEntry.Offset,
		"size", keyEntry.Size)

	// Check if we need to flush unflushed data before reading
	if err := e.ensureDataFlushed(keyEntry); err != nil {
		return "", fmt.Errorf("failed to ensure data flushed: %w", err)
	}

	data, err := e.file.ReadAt(keyEntry.Offset, keyEntry.Size)
	if err != nil {
		return "", fmt.Errorf("failed to read data from file at offset %d: %w", keyEntry.Offset, err)
	}

	record, err := format.Decode(data, e.cfg.HEADER_SIZE)
	if err != nil {
		return "", fmt.Errorf("failed to decode record for key %s: %w", key, err)
	}

	if record.Flag == format.FlagTombstone {
		slog.Debug("get: record is tombstone",
			"key", key)
		return "", errors.New("key not found")
	}

	slog.Info("get: success",
		"key", key,
		"value_size", len(record.Value),
		"timestamp", record.Timestamp)
	return string(record.Value), nil
}

// ensureDataFlushed checks if the data at the given offset is in the buffer
// and flushes it to disk if necessary. This ensures reads can access the data.
func (e *KVEngine) ensureDataFlushed(keyEntry *Key) error {
	// Type assertion to access file-specific methods
	file, ok := e.file.(*storage.File)
	if !ok {
		return nil // Not a File type, skip flush check
	}

	shouldFlush, err := file.ShouldFlushBeforeRead(keyEntry.Offset)
	if err != nil {
		return fmt.Errorf("failed to check if flush needed: %w", err)
	}

	if shouldFlush {
		if err := e.file.Flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
	}

	return nil
}

// Put stores a key-value pair in the database.
// It encodes the record, appends it to the log file, and updates the
// in-memory key directory. Returns an error if encoding or I/O fails.
func (e *KVEngine) Put(key string, value string) error {
	record := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   uint32(len(key)),
		Valuesize: uint32(len(value)),
		Flag:      format.FlagNormal,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	data, err := record.Encode(e.cfg.HEADER_SIZE)
	if err != nil {
		return fmt.Errorf("failed to encode record for key %s: %w", key, err)
	}
	commitRecord := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   0,
		Valuesize: 0,
		Flag:      format.FlagCommit,
		Key:       []byte{},
		Value:     nil,
	}
	commitData, err := commitRecord.Encode(e.cfg.HEADER_SIZE)
	if err != nil {
		return fmt.Errorf("failed to encode commit record: %w", err)
	}

	offset, err := e.file.Append(append(data, commitData...))
	if err != nil {
		return fmt.Errorf("failed to append data to file for key %s: %w", key, err)
	}

	recordSize := record.Valuesize + record.Keysize + e.cfg.HEADER_SIZE
	keyEntry := &Key{
		FileId: 0, // Single file implementation
		Size:   recordSize,
		Offset: offset,
	}

	e.keyDir.Store(key, keyEntry)

	slog.Info("put: success",
		"key", key,
		"offset", offset,
		"record_size", recordSize,
		"key_size", len(key),
		"value_size", len(value),
		"timestamp", record.Timestamp)
	return nil
}

// Delete removes a key from the database by writing a tombstone marker.
// The key is removed from the in-memory key directory immediately.
// Returns an error if encoding or I/O fails.
func (e *KVEngine) Delete(key string) error {
	record := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   uint32(len(key)),
		Valuesize: 0,
		Flag:      format.FlagTombstone,
		Key:       []byte(key),
		Value:     nil,
	}

	data, err := record.Encode(e.cfg.HEADER_SIZE)
	if err != nil {
		return fmt.Errorf("failed to encode tombstone record for key %s: %w", key, err)
	}

	offset, err := e.file.Append(data)
	if err != nil {
		return fmt.Errorf("failed to append tombstone to file for key %s: %w", key, err)
	}

	e.keyDir.Delete(key)

	slog.Info("delete: success",
		"key", key,
		"offset", offset)
	return nil
}

// Close gracefully shuts down the KV engine, flushing any pending writes
// and closing the storage file. Returns an error if closing fails.
func (e *KVEngine) Close() error {
	if e.file != nil {
		keyCount := e.GetKeyDirSize()

		slog.Info("engine: closing KV engine",
			"keys_in_memory", keyCount)
		return e.file.Close()
	}
	slog.Warn("engine: close called but file handler is nil")
	return nil
}

// GetKeyDirSize returns the number of keys currently in the in-memory key directory.
func (e *KVEngine) GetKeyDirSize() int {
	count := 0
	e.keyDir.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// RecoverKeyDir rebuilds the in-memory key directory by scanning the log file
// from the beginning. It processes all records, handling tombstones appropriately,
// and reconstructs the key-to-offset mapping. Returns an error if recovery fails.
func (e *KVEngine) RecoverKeyDir() error {
	file, ok := e.file.(*storage.File)
	if !ok {
		return fmt.Errorf("file interface is not a File type, cannot recover")
	}

	reader := bufio.NewReader(file.GetFile())
	count, err := e.scanLogFile(reader)
	if err != nil {
		return fmt.Errorf("failed to scan log file: %w", err)
	}

	slog.Info("recoverKeyDir: recovered keyDir",
		"size", count)

	return nil
}

// scanLogFile scans the entire log file and rebuilds the key directory.
// It processes records sequentially, handling tombstones and normal records.
// Returns the count of recovered keys and any error encountered.
func (e *KVEngine) scanLogFile(reader *bufio.Reader) (int, error) {
	count := 0
	currentOffset := int64(0)

	type recordWithOffsetAndSize struct {
		record *format.Record
		offset int64
		size   int
	}

	recordsToCommit := make([]recordWithOffsetAndSize, 0)

	for {
		record, recordSize, err := e.readNextRecord(reader, currentOffset)
		if err == io.EOF {
			break // End of file reached normally
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read record at offset %d: %w", currentOffset, err)
		}

		if record.Flag == format.FlagCommit {
			for _, record := range recordsToCommit {
				if e.processRecoveredRecord(record.record, record.offset, record.size) {
					count++
				}
			}
			recordsToCommit = make([]recordWithOffsetAndSize, 0)
		} else {
			recordsToCommit = append(recordsToCommit, recordWithOffsetAndSize{
				record: record,
				size:   recordSize,
			})
		}

		currentOffset += int64(recordSize)
	}

	return count, nil
}

// processRecoveredRecord processes a single recovered record, updating the
// key directory appropriately based on whether it's a tombstone or normal record.
// Returns true if a key was added (not a tombstone), false otherwise.
func (e *KVEngine) processRecoveredRecord(record *format.Record, offset int64, size int) bool {
	key := string(record.Key)
	if record.Flag == format.FlagTombstone {
		slog.Debug("recoverKeyDir: tombstone record detected",
			"key", key)
		e.keyDir.Delete(key)
		return false
	}

	e.keyDir.Store(key, &Key{
		FileId: 0,
		Size:   uint32(size),
		Offset: offset,
	})
	return true
}

// readNextRecord reads a single record from the reader, handling incomplete
// records at the end of the file. Returns the decoded record, its total size,
// and any error encountered.
func (e *KVEngine) readNextRecord(reader *bufio.Reader, currentOffset int64) (*format.Record, int, error) {
	headerBuf := make([]byte, e.cfg.HEADER_SIZE)
	_, err := io.ReadFull(reader, headerBuf)
	if err != nil {
		return nil, 0, err
	}

	keySize := binary.LittleEndian.Uint32(headerBuf[12:16])
	valSize := binary.LittleEndian.Uint32(headerBuf[16:20])
	totalRecordSize := int(e.cfg.HEADER_SIZE) + int(keySize) + int(valSize)

	bodyBuf := make([]byte, keySize+valSize)
	bytesRead, err := io.ReadFull(reader, bodyBuf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// Incomplete record - file was likely truncated or write was interrupted
		slog.Warn("recoverKeyDir: incomplete record detected at end of file, stopping recovery",
			"offset", currentOffset,
			"expected_body_size", keySize+valSize,
			"bytes_read", bytesRead)
		return nil, 0, io.EOF
	}
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read record body: %w", err)
	}

	fullRecord := append(headerBuf, bodyBuf...)
	record, err := format.Decode(fullRecord, e.cfg.HEADER_SIZE)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode record: %w", err)
	}

	return record, totalRecordSize, nil
}
