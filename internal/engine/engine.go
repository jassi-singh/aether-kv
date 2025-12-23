package engine

import (
	"errors"
	"log/slog"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/format"
)

type Engine interface {
	Get(key string) (string, error)
	Put(key string, value string) error
	Delete(key string) error
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

	data, err := e.file.ReadAt(keyEntry.Offset, keyEntry.Size)
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
	// implementation for deleting a key-value pair
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
