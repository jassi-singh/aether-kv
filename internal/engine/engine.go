package engine

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/format"
)

type Engine interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
	Close() error
	GetKeyDirSize() int
	RecoverKeyDir() error
}

type KVEngine struct {
	keyDir map[string]*Key
	file   *File
}

func NewKVEngine() (*KVEngine, error) {
	slog.Info("engine: initializing KV engine")

	file, err := NewFile()
	if err != nil {
		slog.Error("engine: failed to create file handler",
			"error", err)
		return nil, err
	}

	engine := &KVEngine{
		keyDir: NewKeyDir(),
		file:   file,
	}

	err = engine.RecoverKeyDir()
	if err != nil {
		slog.Error("engine: failed to recover keyDir",
			"error", err)
		return nil, err
	}

	slog.Info("engine: KV engine initialized successfully")
	return engine, nil
}

func (e *KVEngine) Get(key string) (string, error) {
	keyEntry, ok := e.keyDir[key]
	if !ok {
		slog.Debug("get: key not found in keyDir",
			"key", key)
		return "", errors.New("key not found")
	}

	slog.Debug("get: reading record from file",
		"key", key,
		"offset", keyEntry.Offset,
		"size", keyEntry.Size)

	fileSize, err := e.file.file.Seek(0, io.SeekEnd)
	if err != nil {
		slog.Error("get: failed to seek to end of file",
			"error", err)
		return "", err
	}

	bufferedSize := int64(e.file.buffer.Buffered())
	unflushedStart := fileSize

	if keyEntry.Offset >= unflushedStart && keyEntry.Offset < unflushedStart+bufferedSize {
		if err := e.file.flushAndSync(); err != nil {
			return "", err
		}
	}

	data, err := e.file.ReadAt(keyEntry.Offset, int64(keyEntry.Size))
	if err != nil {
		slog.Error("get: failed to read data from file",
			"key", key,
			"offset", keyEntry.Offset,
			"size", keyEntry.Size,
			"error", err)
		return "", err
	}

	record, err := format.Decode(data)
	if err != nil {
		slog.Error("get: failed to decode record",
			"key", key,
			"offset", keyEntry.Offset,
			"size", keyEntry.Size,
			"error", err)
		return "", err
	}

	// Check if record is a tombstone (future implementation)
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

func (e *KVEngine) Put(key string, value string) error {
	record := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   uint32(len(key)),
		Valuesize: uint32(len(value)),
		Flag:      format.FlagNormal,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	data, err := record.Encode()
	if err != nil {
		slog.Error("put: failed to encode record",
			"key", key,
			"key_size", len(key),
			"value_size", len(value),
			"error", err)
		return err
	}

	offset, err := e.file.Append(data)
	if err != nil {
		slog.Error("put: failed to append data to file",
			"key", key,
			"data_size", len(data),
			"error", err)
		return err
	}

	cfg := config.GetConfig()
	recordSize := record.Valuesize + record.Keysize + cfg.HEADER_SIZE
	e.keyDir[key] = &Key{
		FileId: 0, // assuming single file for simplicity
		Size:   recordSize,
		Offset: offset,
	}

	slog.Info("put: success",
		"key", key,
		"offset", offset,
		"record_size", recordSize,
		"key_size", len(key),
		"value_size", len(value),
		"timestamp", record.Timestamp)
	return nil
}

func (e *KVEngine) Delete(key string) error {
	record := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   uint32(len(key)),
		Valuesize: 0,
		Flag:      format.FlagTombstone,
		Key:       []byte(key),
		Value:     nil,
	}

	data, err := record.Encode()
	if err != nil {
		slog.Error("delete: failed to encode record",
			"key", key,
			"key_size", len(key),
			"error", err)
		return err
	}

	offset, err := e.file.Append(data)
	if err != nil {
		slog.Error("delete: failed to append data to file",
			"key", key,
			"data_size", len(data),
			"error", err)
		return err
	}

	delete(e.keyDir, key)

	slog.Info("delete: success",
		"key", key,
		"offset", offset)
	return nil
}

func (e *KVEngine) Close() error {
	if e.file != nil {
		slog.Info("engine: closing KV engine",
			"keys_in_memory", len(e.keyDir))
		return e.file.Close()
	}
	slog.Warn("engine: close called but file handler is nil")
	return nil
}

func (e *KVEngine) GetKeyDirSize() int {
	return len(e.keyDir)
}

func (e *KVEngine) RecoverKeyDir() error {
	recoveredKeyDir := make(map[string]*Key)
	cfg := config.GetConfig()

	reader := bufio.NewReader(e.file.file)
	currentOffset := int64(0)

	for {
		headerBuf := make([]byte, cfg.HEADER_SIZE)
		_, err := io.ReadFull(reader, headerBuf)
		if err == io.EOF {
			break // End of file reached normally
		}
		if err != nil {
			slog.Error("recoverKeyDir: failed to read header",
				"error", err)
			return err
		}

		keySize := binary.LittleEndian.Uint32(headerBuf[12:16])
		valSize := binary.LittleEndian.Uint32(headerBuf[16:20])
		totalRecordSize := cfg.HEADER_SIZE + keySize + valSize

		bodyBuf := make([]byte, keySize+valSize)
		bytesRead, err := io.ReadFull(reader, bodyBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Incomplete record - file was likely truncated or write was interrupted
			slog.Warn("recoverKeyDir: incomplete record detected at end of file, stopping recovery",
				"offset", currentOffset,
				"expected_body_size", keySize+valSize,
				"bytes_read", bytesRead)
			break // Stop recovery - we've reached the end of valid data
		}
		if err != nil {
			slog.Error("recoverKeyDir: failed to read body",
				"offset", currentOffset,
				"error", err)
			return err
		}

		fullRecord := append(headerBuf, bodyBuf...)
		record, err := format.Decode(fullRecord)
		if err != nil {
			slog.Error("recoverKeyDir: failed to decode record",
				"error", err)
			return err
		}

		if record.Flag == format.FlagTombstone {
			slog.Debug("recoverKeyDir: tombstone record detected",
				"key", string(record.Key))
			delete(recoveredKeyDir, string(record.Key))
		} else {
			recoveredKeyDir[string(record.Key)] = &Key{
				FileId: 0,
				Size:   uint32(totalRecordSize),
				Offset: currentOffset,
			}
		}

		currentOffset += int64(totalRecordSize)
	}

	slog.Info("recoverKeyDir: recovered keyDir",
		"size", len(recoveredKeyDir))

	e.keyDir = recoveredKeyDir
	return nil
}
