package main

import (
	"bufio"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/engine"
)

func main() {
	// Initialize structured logger
	// Use JSON handler for production, or TextHandler for development
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo, // Change to LevelDebug for verbose logging
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Load configuration
	slog.Info("main: loading configuration")
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("main: failed to load configuration",
			"error", err)
		log.Fatalf("Failed to load config: %v", err)
	}
	slog.Info("main: configuration loaded successfully",
		"data_dir", cfg.DATA_DIR,
		"header_size", cfg.HEADER_SIZE,
		"batch_size", cfg.BATCH_SIZE,
		"sync_interval", cfg.SYNC_INTERVAL,
	)

	kv, err := engine.NewKVEngine()
	if err != nil {
		slog.Error("main: failed to initialize KV engine",
			"error", err)
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			slog.Error("main: error closing KV engine",
				"error", err)
		}
	}()

	slog.Info("main: Aether KV started successfully")

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Aether KV - Simple Key-Value Store")
	fmt.Println("Commands: PUT <key> <value>, GET <key>, DELETE <key>, EXIT")
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			fmt.Print("> ")
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "PUT":
			if len(parts) < 3 {
				slog.Warn("main: invalid PUT command - missing arguments")
				fmt.Println("Usage: PUT <key> <value>")
			} else {
				key := parts[1]
				value := strings.Join(parts[2:], " ")
				slog.Debug("main: executing PUT command",
					"key", key,
					"value_size", len(value))
				if err := kv.Put(key, value); err != nil {
					slog.Error("main: PUT command failed",
						"key", key,
						"value_size", len(value),
						"error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("OK\n")
				}
			}

		case "GET":
			if len(parts) < 2 {
				slog.Warn("main: invalid GET command - missing key")
				fmt.Println("Usage: GET <key>")
			} else {
				key := parts[1]
				slog.Debug("main: executing GET command",
					"key", key)
				value, err := kv.Get(key)
				if err != nil {
					slog.Debug("main: GET command failed",
						"key", key,
						"error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("%s\n", value)
				}
			}

		case "DELETE":
			if len(parts) < 2 {
				slog.Warn("main: invalid DELETE command - missing key")
				fmt.Println("Usage: DELETE <key>")
			} else {
				key := parts[1]
				slog.Debug("main: executing DELETE command",
					"key", key)
				if err := kv.Delete(key); err != nil {
					slog.Error("main: DELETE command failed",
						"key", key,
						"error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Println("OK")
				}
			}

		case "EXIT", "QUIT":
			slog.Info("main: shutdown requested by user")
			fmt.Println("Goodbye!")
			return

		default:
			slog.Warn("main: unknown command received",
				"command", command)
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Commands: PUT <key> <value>, GET <key>, DELETE <key>, EXIT")
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		slog.Error("main: error reading input",
			"error", err)
		log.Fatalf("Error reading input: %v", err)
	}
}
