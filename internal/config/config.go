package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

// Config holds the application configuration values

type Config struct {
	DATA_DIR string `yaml:"DATA_DIR"`
	HEADER_SIZE int `yaml:"HEADER_SIZE"`
}

// LoadConfig reads configuration values from config.yml
func LoadConfig() (*Config, error) {
	file, err := os.ReadFile("config/config.yml")
	if err != nil {
		return nil, err
	}

	var appConfig Config
	err = yaml.Unmarshal([]byte(os.ExpandEnv(string(file))), &appConfig)

	if err != nil {
		return nil, err
	}

	return &appConfig, nil
}