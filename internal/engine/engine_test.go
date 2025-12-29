// Package engine provides unit tests for the key-value storage engine.
package engine

import (
	"fmt"
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

// cleanupTestFiles removes test files if they exist.
func cleanupTestFiles(cfg *config.Config) {
	filePath := filepath.Join(cfg.DATA_DIR, "active.log")
	os.Remove(filePath)
}

func TestNewKVEngine(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

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
			engine, err := NewKVEngine(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKVEngine() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && engine == nil {
				t.Error("NewKVEngine() returned nil engine without error")
			}
			if engine != nil {
				engine.Close()
			}
		})
	}
}

func TestKVEngine_Put(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{
			name:    "simple put",
			key:     "test-key",
			value:   "test-value",
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     "",
			value:   "value",
			wantErr: false,
		},
		{
			name:    "empty value",
			key:     "key",
			value:   "",
			wantErr: false,
		},
		{
			name:    "large value",
			key:     "key",
			value:   string(make([]byte, 1000)),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := engine.Put(tt.key, tt.value); (err != nil) != tt.wantErr {
				t.Errorf("KVEngine.Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestKVEngine_Get(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Put a value first
	key := "test-key"
	value := "test-value"
	if err := engine.Put(key, value); err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{
			name:    "existing key",
			key:     key,
			want:    value,
			wantErr: false,
		},
		{
			name:    "non-existent key",
			key:     "non-existent",
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := engine.Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("KVEngine.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("KVEngine.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKVEngine_Delete(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Put a value first
	key := "test-key"
	value := "test-value"
	if err := engine.Put(key, value); err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Delete the key
	if err := engine.Delete(key); err != nil {
		t.Errorf("KVEngine.Delete() error = %v", err)
	}

	// Verify it's deleted
	_, err = engine.Get(key)
	if err == nil {
		t.Error("KVEngine.Delete() did not delete the key")
	}
}

func TestKVEngine_GetKeyDirSize(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Initially should be empty (or recovered keys)
	initialSize := engine.GetKeyDirSize()

	// Add some keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := engine.Put(key, "value"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	finalSize := engine.GetKeyDirSize()
	if finalSize != initialSize+5 {
		t.Errorf("GetKeyDirSize() = %v, want %v", finalSize, initialSize+5)
	}
}

func TestKVEngine_ConcurrentOperations(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Test concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := "concurrent-key"
			value := "value"
			if err := engine.Put(key, value); err != nil {
				t.Errorf("Concurrent Put failed: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify we can still read
	_, err = engine.Get("concurrent-key")
	if err != nil {
		t.Errorf("Failed to get after concurrent operations: %v", err)
	}
}

func TestKVEngine_RecoverKeyDir(t *testing.T) {
	cfg := setupTestConfig(t)
	defer cleanupTestFiles(cfg)

	engine, err := NewKVEngine(cfg)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Add some keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := engine.Put(key, "value"); err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	// Recover the key directory
	err = engine.RecoverKeyDir()
	if err != nil {
		t.Fatalf("Failed to recover key directory: %v", err)
	}

	// Verify the key directory is recovered
	size := engine.GetKeyDirSize()
	if size != 5 {
		t.Errorf("GetKeyDirSize() = %v, want %v", size, 5)
	}

	// Verify we can read the keys
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		got, err := engine.Get(key)
		if err != nil {
			t.Errorf("Failed to get key: %v", err)
		}
		if got != "value" {
			t.Errorf("Get() = %v, want %v", got, "value")
		}
	}
}
