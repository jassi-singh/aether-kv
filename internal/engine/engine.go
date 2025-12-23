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
	file, err := NewFile()
	if err != nil {
		return nil, err
	}

	return &KVEngine{
		keyDir: NewKeyDir(),
		file:   file,
	}, nil
}

func (e *KVEngine) Get(key string) (string, error) {
	keyEntry, ok := e.keyDir[key]
	if !ok {
		slog.Debug("get: key not found", "key", key)
		return "", errors.New("key not found")
	}

	data, err := e.file.ReadAt(keyEntry.Offset, keyEntry.Size)
	if err != nil {
		slog.Error("get: error reading data",
			"key", key,
			"offset", keyEntry.Offset,
			"size", keyEntry.Size,
			"error", err)
		return "", err
	}

	record, err := format.Decode(data)
	if err != nil {
		slog.Error("get: error decoding data",
			"key", key,
			"offset", keyEntry.Offset,
			"error", err)
		return "", err
	}

	slog.Debug("get: success", "key", key)
	return string(record.Value), nil
}

func (e *KVEngine) Put(key string, value string) error {
	record := &format.Record{
		Timestamp: uint64(time.Now().Unix()),
		Keysize:   uint32(len(key)),
		Valuesize: uint32(len(value)),
		Flag:      0,
		Key:       []byte(key),
		Value:     []byte(value),
	}

	data, err := record.Encode()
	if err != nil {
		slog.Error("put: error encoding record",
			"key", key,
			"error", err)
		return err
	}

	offset, err := e.file.Append(data)
	if err != nil {
		slog.Error("put: error appending to file",
			"key", key,
			"error", err)
		return err
	}

	cfg := config.GetConfig()
	e.keyDir[key] = &Key{
		FileId: 0, // assuming single file for simplicity
		Size:   record.Valuesize + record.Keysize + cfg.HEADER_SIZE,
		Offset: offset,
	}

	slog.Debug("put: success",
		"key", key,
		"offset", offset,
		"value_size", len(value))
	return nil
}

func (e *KVEngine) Delete(key string) error {
	// implementation for deleting a key-value pair
	return nil
}

func (e *KVEngine) Close() error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}
