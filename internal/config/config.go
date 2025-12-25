package config

import (
	"log/slog"
	"os"
	"sync"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v2"
)

// Config holds the application configuration values

type Config struct {
	DATA_DIR      string `yaml:"DATA_DIR"`
	HEADER_SIZE   uint32 `yaml:"HEADER_SIZE"`
	BATCH_SIZE    uint32 `yaml:"BATCH_SIZE"`
	SYNC_INTERVAL uint32 `yaml:"SYNC_INTERVAL"`
}

var (
	appConfig *Config
	once      sync.Once
	initErr   error
)

// LoadConfig reads configuration values from config.yml
// It automatically loads .env file if it exists (optional, no error if missing)
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

func GetConfig() *Config {
	if appConfig == nil {
		panic("config not loaded")
	}
	return appConfig
}
