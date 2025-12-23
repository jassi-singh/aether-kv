package main

import (
	"bufio"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

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

	kv, err := engine.NewKVEngine()
	if err != nil {
		slog.Error("Failed to create KV engine", "error", err)
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer kv.Close()

	slog.Info("KV engine initialized successfully")

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
				fmt.Println("Usage: PUT <key> <value>")
			} else {
				key := parts[1]
				value := strings.Join(parts[2:], " ")
				if err := kv.Put(key, value); err != nil {
					slog.Error("put command failed", "key", key, "error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					slog.Info("put command succeeded", "key", key)
					fmt.Printf("OK\n")
				}
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
			} else {
				key := parts[1]
				value, err := kv.Get(key)
				if err != nil {
					slog.Debug("get command failed", "key", key, "error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					slog.Debug("get command succeeded", "key", key)
					fmt.Printf("%s\n", value)
				}
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
			} else {
				key := parts[1]
				if err := kv.Delete(key); err != nil {
					slog.Error("delete command failed", "key", key, "error", err)
					fmt.Printf("Error: %v\n", err)
				} else {
					slog.Info("delete command succeeded", "key", key)
					fmt.Println("OK")
				}
			}

		case "EXIT", "QUIT":
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Commands: PUT <key> <value>, GET <key>, DELETE <key>, EXIT")
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %v", err)
	}
}
