package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log/slog"

	"github.com/jassi-singh/aether-kv/internal/config"
)

// Record flag constants
const (
	FlagNormal    uint8 = 0 // Normal log entry
	FlagTombstone uint8 = 1 // Tombstone marker for deleted entries
)

type Record struct {
	CRC       uint32
	Timestamp uint64
	Keysize   uint32
	Valuesize uint32
	Flag      uint8
	Key       []byte
	Value     []byte
}

type Codec interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

func (r *Record) Encode() ([]byte, error) {
	cfg := config.GetConfig()
	buffer := make([]byte, int(cfg.HEADER_SIZE)+len(r.Key)+len(r.Value))

	binary.LittleEndian.PutUint64(buffer[4:12], r.Timestamp)
	binary.LittleEndian.PutUint32(buffer[12:16], r.Keysize)
	binary.LittleEndian.PutUint32(buffer[16:20], r.Valuesize)
	buffer[20] = r.Flag

	copy(buffer[cfg.HEADER_SIZE:int(cfg.HEADER_SIZE)+len(r.Key)], r.Key)
	copy(buffer[int(cfg.HEADER_SIZE)+len(r.Key):], r.Value)

	crc := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[0:4], crc)

	return buffer, nil
}

func Decode(data []byte) (*Record, error) {
	cfg := config.GetConfig()
	if len(data) < int(cfg.HEADER_SIZE) {
		slog.Error("decode: data too short - insufficient header data",
			"actual_length", len(data),
			"expected_min", cfg.HEADER_SIZE)
		return nil, errors.New("data too short: minimum header size required")
	}

	CRC := binary.LittleEndian.Uint32(data[0:4])
	Timestamp := binary.LittleEndian.Uint64(data[4:12])
	Keysize := binary.LittleEndian.Uint32(data[12:16])
	Valuesize := binary.LittleEndian.Uint32(data[16:20])
	Flag := data[20]

	record := &Record{
		CRC:       CRC,
		Timestamp: Timestamp,
		Keysize:   Keysize,
		Valuesize: Valuesize,
		Flag:      Flag,
		Key:       data[cfg.HEADER_SIZE : cfg.HEADER_SIZE+Keysize],
		Value:     data[cfg.HEADER_SIZE+Keysize : cfg.HEADER_SIZE+Keysize+Valuesize],
	}

	expectedLength := int(cfg.HEADER_SIZE + Keysize + Valuesize)
	if len(data) < expectedLength {
		slog.Error("decode: insufficient data - incomplete record",
			"actual_length", len(data),
			"expected_length", expectedLength,
			"keysize", Keysize,
			"valuesize", Valuesize,
			"header_size", cfg.HEADER_SIZE)
		return nil, errors.New("data too short: insufficient data for key and value")
	}

	calculatedCRC := crc32.ChecksumIEEE(data[4:expectedLength])
	if calculatedCRC != CRC {
		slog.Error("decode: CRC mismatch - data corruption detected",
			"calculated_crc", calculatedCRC,
			"expected_crc", CRC,
			"data_length", expectedLength,
			"keysize", Keysize,
			"valuesize", Valuesize)
		return nil, errors.New("crc mismatch: data corruption detected")
	}

	// Log tombstone records for debugging
	if Flag == FlagTombstone {
		slog.Debug("decode: tombstone record detected",
			"key", string(record.Key),
			"timestamp", Timestamp)
	}

	return record, nil
}
