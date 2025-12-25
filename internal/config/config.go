// Package config provides configuration management for the key-value store.
// It loads settings from YAML files and environment variables, with
// thread-safe singleton access.
package config

import (
	"log/slog"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v2"
)

// Config holds all application configuration values.
type Config struct {
	DATA_DIR      string `yaml:"DATA_DIR"`      // Directory where log files are stored
	HEADER_SIZE   uint32 `yaml:"HEADER_SIZE"`   // Size of record header in bytes
	BATCH_SIZE    uint32 `yaml:"BATCH_SIZE"`    // Buffer size threshold for auto-flush
	SYNC_INTERVAL uint32 `yaml:"SYNC_INTERVAL"` // Time interval in seconds for auto-sync
}

var (
	appConfig *Config
	once      sync.Once
	initErr   error
)

// LoadConfig reads configuration values from config.yml and optionally from .env file.
// It uses a sync.Once to ensure configuration is loaded only once, even with
// concurrent calls. Environment variables in the YAML file are expanded using
// os.ExpandEnv. Returns the loaded configuration and any error encountered.
func LoadConfig() (*Config, error) {
	once.Do(func() {
		// Load .env file if it exists (optional - no error if missing)
		if err := godotenv.Load(); err != nil {
			slog.Debug("No .env file found or error loading it", "error", err)
		} else {
			slog.Debug(".env file loaded successfully")
		}

		file, err := os.ReadFile("internal/config/config.yml")
		if err != nil {
			initErr = err
			return
		}

		var cfg Config
		err = yaml.Unmarshal([]byte(os.ExpandEnv(string(file))), &cfg)
		if err != nil {
			initErr = err
			return
		}
		appConfig = &cfg
	})
	if initErr != nil {
		return nil, initErr
	}
	return appConfig, initErr
}

// GetConfig returns the singleton configuration instance.
// Panics if configuration has not been loaded yet. This function should
// only be called after LoadConfig has been successfully called.
func GetConfig() *Config {
	if appConfig == nil {
		panic("config not loaded - call LoadConfig() first")
	}
	return appConfig
}
