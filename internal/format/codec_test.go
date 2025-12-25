// Package format provides unit tests for record encoding and decoding.
package format

import (
	"testing"
)

const testHeaderSize = uint32(21)

func setupTestConfig(t *testing.T) {
	// No need to load config for format tests - we use a constant header size
}

func TestRecord_Encode(t *testing.T) {
	setupTestConfig(t)

	tests := []struct {
		name    string
		record  *Record
		wantErr bool
	}{
		{
			name: "normal record",
			record: &Record{
				Timestamp: 1234567890,
				Keysize:   3,
				Valuesize: 5,
				Flag:      FlagNormal,
				Key:       []byte("key"),
				Value:     []byte("value"),
			},
			wantErr: false,
		},
		{
			name: "tombstone record",
			record: &Record{
				Timestamp: 1234567890,
				Keysize:   3,
				Valuesize: 0,
				Flag:      FlagTombstone,
				Key:       []byte("key"),
				Value:     nil,
			},
			wantErr: false,
		},
		{
			name: "empty key",
			record: &Record{
				Timestamp: 1234567890,
				Keysize:   0,
				Valuesize: 5,
				Flag:      FlagNormal,
				Key:       []byte{},
				Value:     []byte("value"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.record.Encode(testHeaderSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Record.Encode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(data) == 0 {
				t.Error("Record.Encode() returned empty data")
			}
		})
	}
}

func TestDecode(t *testing.T) {
	setupTestConfig(t)

	// Create and encode a test record
	originalRecord := &Record{
		Timestamp: 1234567890,
		Keysize:   3,
		Valuesize: 5,
		Flag:      FlagNormal,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	encoded, err := originalRecord.Encode(testHeaderSize)
	if err != nil {
		t.Fatalf("Failed to encode record: %v", err)
	}

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "valid encoded data",
			data:    encoded,
			wantErr: false,
		},
		{
			name:    "too short data",
			data:    []byte{1, 2, 3},
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record, err := Decode(tt.data, testHeaderSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && record == nil {
				t.Error("Decode() returned nil record without error")
			}
		})
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	setupTestConfig(t)

	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "normal record",
			record: &Record{
				Timestamp: 1234567890,
				Keysize:   3,
				Valuesize: 5,
				Flag:      FlagNormal,
				Key:       []byte("key"),
				Value:     []byte("value"),
			},
		},
		{
			name: "tombstone record",
			record: &Record{
				Timestamp: 1234567890,
				Keysize:   3,
				Valuesize: 0,
				Flag:      FlagTombstone,
				Key:       []byte("key"),
				Value:     nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded, err := tt.record.Encode(testHeaderSize)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}

			// Decode
			decoded, err := Decode(encoded, testHeaderSize)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}

			// Verify fields
			if decoded.Timestamp != tt.record.Timestamp {
				t.Errorf("Timestamp = %v, want %v", decoded.Timestamp, tt.record.Timestamp)
			}
			if decoded.Keysize != tt.record.Keysize {
				t.Errorf("Keysize = %v, want %v", decoded.Keysize, tt.record.Keysize)
			}
			if decoded.Valuesize != tt.record.Valuesize {
				t.Errorf("Valuesize = %v, want %v", decoded.Valuesize, tt.record.Valuesize)
			}
			if decoded.Flag != tt.record.Flag {
				t.Errorf("Flag = %v, want %v", decoded.Flag, tt.record.Flag)
			}
			if string(decoded.Key) != string(tt.record.Key) {
				t.Errorf("Key = %v, want %v", decoded.Key, tt.record.Key)
			}
			if string(decoded.Value) != string(tt.record.Value) {
				t.Errorf("Value = %v, want %v", decoded.Value, tt.record.Value)
			}
		})
	}
}

func TestDecode_CRCValidation(t *testing.T) {
	setupTestConfig(t)

	// Create a valid record
	record := &Record{
		Timestamp: 1234567890,
		Keysize:   3,
		Valuesize: 5,
		Flag:      FlagNormal,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	encoded, err := record.Encode(testHeaderSize)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Corrupt the CRC
	encoded[0] = 0xFF
	encoded[1] = 0xFF
	encoded[2] = 0xFF
	encoded[3] = 0xFF

	// Decode should fail with CRC mismatch
	_, err = Decode(encoded, testHeaderSize)
	if err == nil {
		t.Error("Decode() should have failed with corrupted CRC")
	}
}
