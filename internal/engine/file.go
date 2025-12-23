package engine

import (
	"io"
	"log/slog"
	"os"

	"github.com/jassi-singh/aether-kv/internal/config"
)

type FileInterface interface {
	Append(data []byte) (int64, error)
	ReadAt(offset int64, size uint32) ([]byte, error)
	Close() error
}

type File struct {
	file *os.File
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
		file: file,
	}, nil
}

func (f *File) Append(data []byte) (int64, error) {
	offset, err := f.file.Seek(0, io.SeekEnd)
	if err != nil {
		slog.Error("file: failed to seek to end of file",
			"error", err)
		return 0, err
	}

	bytesWritten, err := f.file.Write(data)
	if err != nil {
		slog.Error("file: failed to write data",
			"offset", offset,
			"data_size", len(data),
			"error", err)
		return 0, err
	}

	if bytesWritten != len(data) {
		slog.Warn("file: partial write detected",
			"expected", len(data),
			"written", bytesWritten,
			"offset", offset)
	}

	err = f.file.Sync()
	if err != nil {
		slog.Error("file: failed to sync data to disk",
			"offset", offset,
			"error", err)
		return 0, err
	}

	slog.Debug("file: data appended successfully",
		"offset", offset,
		"size", len(data))
	return offset, nil
}

func (f *File) ReadAt(offset int64, size uint32) ([]byte, error) {
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
	err := f.file.Close()
	if err != nil {
		slog.Error("file: failed to close file",
			"error", err)
		return err
	}
	slog.Info("file: file handler closed successfully")
	return nil
}
