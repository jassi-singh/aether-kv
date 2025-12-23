package config

import (
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

// Config holds the application configuration values

type Config struct {
	DATA_DIR    string `yaml:"DATA_DIR"`
	HEADER_SIZE uint32    `yaml:"HEADER_SIZE"`
}

var (
	appConfig *Config
	once      sync.Once
	initErr   error
)

// LoadConfig reads configuration values from config.yml
func LoadConfig() (*Config, error) {
	once.Do(func() {
		file, err := os.ReadFile("config/config.yml")
		if err != nil {
			initErr = err
			return
		}

		var appConfig Config
		err = yaml.Unmarshal([]byte(os.ExpandEnv(string(file))), &appConfig)
		if err != nil {
			initErr = err
			return
		}
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