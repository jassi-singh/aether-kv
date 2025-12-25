// Package storage provides file storage operations for the key-value store.
// It handles buffered writes, automatic flushing, and file I/O operations.
package storage

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
)

// Storage defines the interface for storage operations.
// This abstraction allows for different storage backends and easier testing.
type Storage interface {
	Append(data []byte) (int64, error)
	ReadAt(offset int64, size uint32) ([]byte, error)
	Close() error
	Flush() error
	// Internal methods for engine coordination
	GetFile() *os.File
	GetBuffer() *bufio.Writer
}

// File implements Storage and provides buffered file operations
// with automatic flushing based on batch size and sync interval.
type File struct {
	mu           sync.Mutex // Protects buffer and file operations from concurrent access
	buffer       *bufio.Writer
	file         *os.File
	lastSyncTime time.Time
	cfg          *config.Config
}

// NewFile creates a new File instance with the given configuration.
// It opens or creates the active log file in append mode and initializes
// the write buffer. Returns an error if file operations fail.
func NewFile(cfg *config.Config) (*File, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Ensure the data directory exists
	if err := os.MkdirAll(cfg.DATA_DIR, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", cfg.DATA_DIR, err)
	}

	// Ensure the data directory exists
	if err := os.MkdirAll(cfg.DATA_DIR, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", cfg.DATA_DIR, err)
	}

	filePath := cfg.DATA_DIR + "/active.log"

	slog.Debug("storage: opening log file",
		"path", filePath,
		"data_dir", cfg.DATA_DIR)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file at %s: %w", filePath, err)
	}

	// Get file stats for logging
	stat, err := file.Stat()
	if err != nil {
		slog.Warn("storage: failed to get file stats",
			"path", filePath,
			"error", err)
	} else {
		slog.Info("storage: log file opened successfully",
			"path", filePath,
			"size", stat.Size())
	}

	return &File{
		file:         file,
		buffer:       bufio.NewWriter(file),
		lastSyncTime: time.Now(),
		cfg:          cfg,
	}, nil
}

// GetFile returns the underlying os.File for internal engine operations.
// Note: Direct access to the file should be done carefully as it's not thread-safe.
// This method is primarily for recovery operations.
func (f *File) GetFile() *os.File {
	return f.file
}

// GetBuffer returns the underlying bufio.Writer for internal engine operations.
// Note: Direct access to the buffer should be done carefully as it's not thread-safe.
// This method is primarily for recovery operations.
func (f *File) GetBuffer() *bufio.Writer {
	return f.buffer
}

// ShouldFlushBeforeRead checks if data at the given offset is in the unflushed buffer
// and returns true if a flush is needed. This is a thread-safe check.
func (f *File) ShouldFlushBeforeRead(offset int64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fileSize, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return false, fmt.Errorf("failed to seek to end of file: %w", err)
	}

	bufferedSize := int64(f.buffer.Buffered())
	unflushedStart := fileSize

	return offset >= unflushedStart && offset < unflushedStart+bufferedSize, nil
}

// Flush flushes the buffer and syncs the file to disk.
// This is exposed for cases where the engine needs to ensure data is persisted.
// This method is thread-safe and can be called concurrently.
func (f *File) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushAndSync()
}

// flushAndSync flushes the write buffer to disk and syncs the file.
// This ensures all buffered data is persisted and updates the last sync time.
// Returns an error if flushing or syncing fails.
func (f *File) flushAndSync() error {
	if err := f.buffer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file after flush: %w", err)
	}

	f.lastSyncTime = time.Now()
	slog.Debug("storage: buffer flushed, file synced, and last sync time updated",
		"last_sync_time", f.lastSyncTime)
	return nil
}

// Append writes data to the log file using a buffered writer.
// It calculates the offset where data will be written (accounting for
// unflushed buffer data) and automatically flushes when batch size or
// sync interval thresholds are reached. Returns the offset where data
// was written and any error encountered.
// This method is thread-safe and can be called concurrently.
func (f *File) Append(data []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fileSize, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to end of file: %w", err)
	}

	// Calculate the actual offset where data will be written
	// This accounts for any unflushed data in the buffer
	bufferSize := int64(f.buffer.Buffered())
	offset := fileSize + bufferSize

	bytesWritten, err := f.buffer.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write data to buffer at offset %d: %w", offset, err)
	}

	if bytesWritten != len(data) {
		slog.Warn("storage: partial buffer write detected",
			"expected", len(data),
			"written", bytesWritten,
			"offset", offset)
	}

	// Auto-flush if batch size or sync interval threshold reached
	if int64(f.buffer.Size()) >= int64(f.cfg.BATCH_SIZE) ||
		time.Since(f.lastSyncTime) >= time.Duration(f.cfg.SYNC_INTERVAL)*time.Second {
		slog.Debug("storage: batch size or sync interval reached, flushing buffer and syncing file",
			"buffer_size", f.buffer.Size(),
			"batch_size", f.cfg.BATCH_SIZE,
			"sync_interval", f.cfg.SYNC_INTERVAL,
			"since_last_sync", time.Since(f.lastSyncTime),
		)
		if err := f.flushAndSync(); err != nil {
			return 0, fmt.Errorf("failed to flush after append: %w", err)
		}
	}
	return offset, nil
}

// ReadAt reads data from the file at the specified offset.
// The size parameter specifies how many bytes to read.
// Returns the read data and any error encountered.
// This method is thread-safe and can be called concurrently.
func (f *File) ReadAt(offset int64, size uint32) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	slog.Debug("storage: reading data from file",
		"offset", offset,
		"size", size)

	data := make([]byte, size)
	bytesRead, err := f.file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read data from file at offset %d: %w", offset, err)
	}

	if bytesRead != int(size) && err != io.EOF {
		slog.Warn("storage: partial read detected",
			"expected", size,
			"read", bytesRead,
			"offset", offset)
	}

	return data, nil
}

// Close gracefully closes the file, flushing any remaining buffered data
// before closing the underlying file handle. Returns an error if closing fails.
// This method is thread-safe and should only be called once.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	slog.Debug("storage: closing file handler")

	// Flush any remaining data in the buffer before closing
	if f.buffer != nil {
		if err := f.flushAndSync(); err != nil {
			slog.Error("storage: failed to flush buffer before close",
				"error", err)
			// Continue to close the file even if flush fails
		}
	}

	if err := f.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	slog.Info("storage: file handler closed successfully")
	return nil
}
