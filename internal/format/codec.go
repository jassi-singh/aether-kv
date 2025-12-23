package format

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log/slog"
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
	buffer := make([]byte, 21+len(r.Key)+len(r.Value))

	binary.LittleEndian.PutUint64(buffer[4:12], r.Timestamp)
	binary.LittleEndian.PutUint32(buffer[12:16], r.Keysize)
	binary.LittleEndian.PutUint32(buffer[16:20], r.Valuesize)
	buffer[20] = r.Flag

	copy(buffer[21:21+len(r.Key)], r.Key)
	copy(buffer[21+len(r.Key):], r.Value)

	crc := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[0:4], crc)

	return buffer, nil
}

func Decode(data []byte) (*Record, error) {
	if len(data) < 21 {
		slog.Debug("decode: data too short",
			"actual_length", len(data),
			"expected_min", 21)
		return nil, errors.New("data too short: minimum 21 bytes required")
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
		Key:       data[21 : 21+Keysize],
		Value:     data[21+Keysize : 21+Keysize+Valuesize],
	}

	expectedLength := 21 + int(Keysize) + int(Valuesize)
	if len(data) < expectedLength {
		slog.Error("decode: insufficient data",
			"actual_length", len(data),
			"expected_length", expectedLength,
			"keysize", Keysize,
			"valuesize", Valuesize)
		return nil, errors.New("data too short: insufficient data for key and value")
	}

	calculatedCRC := crc32.ChecksumIEEE(data[4:expectedLength])
	if calculatedCRC != CRC {
		slog.Error("decode: CRC mismatch",
			"calculated_crc", calculatedCRC,
			"expected_crc", CRC,
			"data_length", expectedLength)
		return nil, errors.New("crc mismatch: data corruption detected")
	}

	return record, nil
}
