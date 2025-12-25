// Package format provides encoding and decoding functionality for key-value records.
// Records are stored in a binary format with CRC checksums for data integrity.
package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log/slog"
)

// Record flag constants define the type of log entry.
const (
	FlagNormal    uint8 = 0 // Normal log entry containing a key-value pair
	FlagTombstone uint8 = 1 // Tombstone marker indicating a deleted entry
)

// Record represents a single key-value entry in the log file.
// It includes metadata (CRC, timestamp, sizes, flag) and the actual key-value data.
type Record struct {
	CRC       uint32 // CRC32 checksum for data integrity verification
	Timestamp uint64 // Unix timestamp when the record was created
	Keysize   uint32 // Size of the key in bytes
	Valuesize uint32 // Size of the value in bytes
	Flag      uint8  // Record type flag (normal or tombstone)
	Key       []byte // The key bytes
	Value     []byte // The value bytes
}

// Codec defines the interface for encoding and decoding records.
type Codec interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

// Encode serializes the record into a byte array with the following format:
// [0:4]   - CRC32 checksum (calculated after encoding other fields)
// [4:12]  - Timestamp (uint64, little-endian)
// [12:16] - Key size (uint32, little-endian)
// [16:20] - Value size (uint32, little-endian)
// [20:21] - Flag (uint8)
// [HEADER_SIZE:] - Key bytes followed by value bytes
// Returns the encoded byte array and any error encountered.
func (r *Record) Encode(headerSize uint32) ([]byte, error) {
	buffer := make([]byte, int(headerSize)+len(r.Key)+len(r.Value))

	binary.LittleEndian.PutUint64(buffer[4:12], r.Timestamp)
	binary.LittleEndian.PutUint32(buffer[12:16], r.Keysize)
	binary.LittleEndian.PutUint32(buffer[16:20], r.Valuesize)
	buffer[20] = r.Flag

	copy(buffer[headerSize:int(headerSize)+len(r.Key)], r.Key)
	copy(buffer[int(headerSize)+len(r.Key):], r.Value)

	crc := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[0:4], crc)

	return buffer, nil
}

// Decode deserializes a byte array into a Record structure.
// It validates the header size, extracts all fields, verifies the CRC checksum,
// and returns the decoded record. Returns an error if the data is invalid or
// corrupted (CRC mismatch).
func Decode(data []byte, headerSize uint32) (*Record, error) {
	if len(data) < int(headerSize) {
		return nil, fmt.Errorf("data too short: got %d bytes, need at least %d bytes for header",
			len(data), headerSize)
	}

	CRC := binary.LittleEndian.Uint32(data[0:4])
	Timestamp := binary.LittleEndian.Uint64(data[4:12])
	Keysize := binary.LittleEndian.Uint32(data[12:16])
	Valuesize := binary.LittleEndian.Uint32(data[16:20])
	Flag := data[20]

	// Validate that we have enough data for the full record
	expectedSize := int(headerSize) + int(Keysize) + int(Valuesize)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("data too short: got %d bytes, need at least %d bytes for full record",
			len(data), expectedSize)
	}

	Key := make([]byte, Keysize)
	Value := make([]byte, Valuesize)
	copy(Key, data[headerSize:headerSize+Keysize])
	copy(Value, data[headerSize+Keysize:headerSize+Keysize+Valuesize])

	record := &Record{
		CRC:       CRC,
		Timestamp: Timestamp,
		Keysize:   Keysize,
		Valuesize: Valuesize,
		Flag:      Flag,
		Key:       Key,
		Value:     Value,
	}

	// Verify CRC checksum
	calculatedCRC := crc32.ChecksumIEEE(data[4:])
	if calculatedCRC != CRC {
		return nil, fmt.Errorf("CRC mismatch: calculated %d, expected %d (data corruption detected)",
			calculatedCRC, CRC)
	}

	// Log tombstone records for debugging
	if Flag == FlagTombstone {
		slog.Debug("decode: tombstone record detected",
			"key", string(record.Key),
			"timestamp", Timestamp)
	}

	return record, nil
}
