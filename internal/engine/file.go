package engine

import (
	"bufio"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
)

type FileInterface interface {
	Append(data []byte) (int64, error)
	ReadAt(offset int64, size uint32) ([]byte, error)
	Close() error
}

type File struct {
	buffer       *bufio.Writer
	file         *os.File
	lastSyncTime time.Time
}

func NewFile() (*File, error) {
	cfg := config.GetConfig()
	filePath := cfg.DATA_DIR + "/active.log"

	slog.Debug("file: opening log file",
		"path", filePath,
		"data_dir", cfg.DATA_DIR)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		slog.Error("file: failed to open log file",
			"path", filePath,
			"error", err)
		return nil, err
	}

	// Get file stats for logging
	stat, err := file.Stat()
	if err != nil {
		slog.Warn("file: failed to get file stats",
			"path", filePath,
			"error", err)
	} else {
		slog.Info("file: log file opened successfully",
			"path", filePath,
			"size", stat.Size())
	}

	return &File{
		file:         file,
		buffer:       bufio.NewWriter(file),
		lastSyncTime: time.Now(),
	}, nil
}

// flushAndSync flushes the buffer to disk and syncs the file
func (f *File) flushAndSync() error {
	err := f.buffer.Flush()
	if err != nil {
		slog.Error("file: failed to flush buffer",
			"error", err)
		return err
	}

	err = f.file.Sync()
	if err != nil {
		slog.Error("file: failed to sync file after flush",
			"error", err)
		return err
	}

	f.lastSyncTime = time.Now()
	slog.Debug("file: buffer flushed, file synced, and last sync time updated",
		"last_sync_time", f.lastSyncTime)
	return nil
}

func (f *File) Append(data []byte) (int64, error) {
	cfg := config.GetConfig()
	// Use buffered writer instead of direct file write
	fileSize, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		slog.Error("file: failed to seek to end of file",
			"error", err)
		return 0, err
	}

	// Calculate the actual offset where data will be written
	// This accounts for any unflushed data in the buffer
	bufferSize := int64(f.buffer.Buffered())
	offset := fileSize + bufferSize

	bytesWritten, err := f.buffer.Write(data)
	if err != nil {
		slog.Error("file: failed to write data to buffer",
			"offset", offset,
			"data_size", len(data),
			"error", err)
		return 0, err
	}

	if bytesWritten != len(data) {
		slog.Warn("file: partial buffer write detected",
			"expected", len(data),
			"written", bytesWritten,
			"offset", offset)
	}

	if int64(f.buffer.Size()) >= int64(cfg.BATCH_SIZE) || time.Since(f.lastSyncTime) >= time.Duration(cfg.SYNC_INTERVAL)*time.Second {
		slog.Warn("file: batch size or sync interval reached, flushing buffer and syncing file",
			"buffer_size", f.buffer.Size(),
			"batch_size", cfg.BATCH_SIZE,
			"sync_interval", cfg.SYNC_INTERVAL,
			"since_last_sync", time.Since(f.lastSyncTime),
		)
		if err := f.flushAndSync(); err != nil {
			return 0, err
		}
	}
	return offset, nil
}

func (f *File) ReadAt(offset int64, size int64) ([]byte, error) {
	slog.Debug("file: reading data from file",
		"offset", offset,
		"size", size)

	data := make([]byte, size)
	bytesRead, err := f.file.ReadAt(data, offset)
	if err != nil {
		slog.Error("file: failed to read data from file",
			"offset", offset,
			"size", size,
			"error", err)
		return nil, err
	}

	if bytesRead != int(size) {
		slog.Warn("file: partial read detected",
			"expected", size,
			"read", bytesRead,
			"offset", offset)
	}

	return data, nil
}

func (f *File) Close() error {
	slog.Debug("file: closing file handler")

	// Flush any remaining data in the buffer before closing
	if f.buffer != nil {
		if err := f.flushAndSync(); err != nil {
			slog.Error("file: failed to flush buffer before close",
				"error", err)
			// Continue to close the file even if flush fails
		}
	}

	err := f.file.Close()
	if err != nil {
		slog.Error("file: failed to close file",
			"error", err)
		return err
	}
	slog.Info("file: file handler closed successfully")
	return nil
}
