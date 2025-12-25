// Package cli provides command-line interface handling for the key-value store.
// It parses user commands and executes them against the storage engine.
package cli

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jassi-singh/aether-kv/internal/engine"
)

// Handler manages the command-line interface for the key-value store.
type Handler struct {
	engine  engine.Engine
	scanner *bufio.Scanner
}

// NewHandler creates a new CLI handler with the given engine.
func NewHandler(e engine.Engine) *Handler {
	return &Handler{
		engine:  e,
		scanner: bufio.NewScanner(os.Stdin),
	}
}

// Run starts the interactive command loop, processing user input until
// an exit command is received or an error occurs.
func (h *Handler) Run() error {
	fmt.Println("Aether KV - Simple Key-Value Store")
	fmt.Println("Commands: PUT <key> <value>, GET <key>, DELETE <key>, EXIT")
	fmt.Print("> ")

	for h.scanner.Scan() {
		line := strings.TrimSpace(h.scanner.Text())
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
			if err := h.handlePut(parts); err != nil {
				return err
			}
		case "GET":
			if err := h.handleGet(parts); err != nil {
				return err
			}
		case "DELETE":
			if err := h.handleDelete(parts); err != nil {
				return err
			}
		case "EXIT", "QUIT":
			slog.Info("cli: shutdown requested by user")
			fmt.Println("Goodbye!")
			return nil
		default:
			slog.Warn("cli: unknown command received",
				"command", command)
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Commands: PUT <key> <value>, GET <key>, DELETE <key>, EXIT")
		}

		fmt.Print("> ")
	}

	if err := h.scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// handlePut processes PUT commands to store key-value pairs.
func (h *Handler) handlePut(parts []string) error {
	if len(parts) < 3 {
		slog.Warn("cli: invalid PUT command - missing arguments")
		fmt.Println("Usage: PUT <key> <value>")
		return nil
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ")

	slog.Debug("cli: executing PUT command",
		"key", key,
		"value_size", len(value))

	if err := h.engine.Put(key, value); err != nil {
		slog.Error("cli: PUT command failed",
			"key", key,
			"value_size", len(value),
			"error", err)
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("OK\n")
	}

	return nil
}

// handleGet processes GET commands to retrieve values by key.
func (h *Handler) handleGet(parts []string) error {
	if len(parts) < 2 {
		slog.Warn("cli: invalid GET command - missing key")
		fmt.Println("Usage: GET <key>")
		return nil
	}

	key := parts[1]
	slog.Debug("cli: executing GET command",
		"key", key)

	value, err := h.engine.Get(key)
	if err != nil {
		slog.Debug("cli: GET command failed",
			"key", key,
			"error", err)
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("%s\n", value)
	}

	return nil
}

// handleDelete processes DELETE commands to remove keys.
func (h *Handler) handleDelete(parts []string) error {
	if len(parts) < 2 {
		slog.Warn("cli: invalid DELETE command - missing key")
		fmt.Println("Usage: DELETE <key>")
		return nil
	}

	key := parts[1]
	slog.Debug("cli: executing DELETE command",
		"key", key)

	if err := h.engine.Delete(key); err != nil {
		slog.Error("cli: DELETE command failed",
			"key", key,
			"error", err)
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Println("OK")
	}

	return nil
}
