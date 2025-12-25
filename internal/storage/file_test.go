// Package storage provides unit tests for file storage operations.
package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jassi-singh/aether-kv/internal/config"
)

// setupTestConfig creates a temporary test configuration.
func setupTestConfig(t *testing.T) *config.Config {
	tmpDir := t.TempDir()
	return &config.Config{
		DATA_DIR:      tmpDir,
		HEADER_SIZE:   21,
		BATCH_SIZE:    4096,
		SYNC_INTERVAL: 5,
	}
}

func TestNewFile(t *testing.T) {
	cfg := setupTestConfig(t)

	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name:    "valid config",
			cfg:     cfg,
			wantErr: false,
		},
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := NewFile(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && file == nil {
				t.Error("NewFile() returned nil file without error")
			}
			if file != nil {
				file.Close()
			}
		})
	}
}

func TestFile_Append(t *testing.T) {
	cfg := setupTestConfig(t)
	file, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "small data",
			data:    []byte("test data"),
			wantErr: false,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: false,
		},
		{
			name:    "large data",
			data:    make([]byte, 1000),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, err := file.Append(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("File.Append() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && offset < 0 {
				t.Errorf("File.Append() returned invalid offset: %d", offset)
			}
		})
	}
}

func TestFile_ReadAt(t *testing.T) {
	cfg := setupTestConfig(t)
	file, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write some data first
	testData := []byte("test data for reading")
	offset, err := file.Append(testData)
	if err != nil {
		t.Fatalf("Failed to append data: %v", err)
	}

	// Flush to ensure data is written
	if err := file.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	tests := []struct {
		name    string
		offset  int64
		size    uint32
		wantErr bool
	}{
		{
			name:    "read valid data",
			offset:  offset,
			size:    uint32(len(testData)),
			wantErr: false,
		},
		{
			name:    "read beyond file",
			offset:  10000,
			size:    10,
			wantErr: false, // ReadAt doesn't error on EOF, just returns partial data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := file.ReadAt(tt.offset, tt.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("File.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(data) != int(tt.size) {
				t.Errorf("File.ReadAt() returned data of length %d, want %d", len(data), tt.size)
			}
		})
	}
}

func TestFile_Close(t *testing.T) {
	cfg := setupTestConfig(t)
	file, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write some data
	if _, err := file.Append([]byte("test")); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	// Close should flush and close successfully
	if err := file.Close(); err != nil {
		t.Errorf("File.Close() error = %v", err)
	}

	// Verify file exists
	filePath := filepath.Join(cfg.DATA_DIR, "active.log")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("File.Close() did not create the log file")
	}
}

func TestFile_Flush(t *testing.T) {
	cfg := setupTestConfig(t)
	file, err := NewFile(cfg)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write some data
	if _, err := file.Append([]byte("test data")); err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	// Flush should succeed
	if err := file.Flush(); err != nil {
		t.Errorf("File.Flush() error = %v", err)
	}
}
