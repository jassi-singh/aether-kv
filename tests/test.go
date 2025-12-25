package main

import (
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jassi-singh/aether-kv/internal/config"
	"github.com/jassi-singh/aether-kv/internal/engine"
)

func main() {
	// Initialize logger
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Load configuration
	_, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	testName := os.Args[1]

	switch testName {
	case "100k-write":
		test100kWrite()
	case "overlapping":
		testOverlappingKey()
	case "integrity":
		testIntegrity()
	default:
		fmt.Printf("Unknown test: %s\n", testName)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: go run tests/test.go <test-name>")
	fmt.Println("\nAvailable tests:")
	fmt.Println("  100k-write  - Write 100,000 unique keys and measure performance")
	fmt.Println("  overlapping - Test overlapping key writes (key_1 with value_A, then value_B)")
	fmt.Println("  integrity   - Write 100k keys, then randomly read 1,000 to verify integrity")
}

// Test 1: 100k Write Test (Speed & Integrity)
func test100kWrite() {
	fmt.Println("=" + strings.Repeat("=", 60))
	fmt.Println("Test 1: 100k Write Test (Speed & Integrity)")
	fmt.Println("=" + strings.Repeat("=", 60))

	kv, err := engine.NewKVEngine()
	if err != nil {
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer kv.Close()

	totalKeys := 100000
	startTime := time.Now()
	errors := 0

	fmt.Printf("Writing %d keys...\n", totalKeys)
	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)

		if err := kv.Put(key, value); err != nil {
			errors++
			if errors <= 10 { // Only print first 10 errors
				fmt.Printf("ERROR: Failed to put key_%d: %v\n", i, err)
			}
		}

		if (i+1)%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d/%d keys written (%.2f keys/sec)\n", i+1, totalKeys, rate)
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(totalKeys) / elapsed.Seconds()

	fmt.Println("\n" + strings.Repeat("-", 60))
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Write rate: %.2f keys/second\n", rate)
	fmt.Printf("Errors: %d\n", errors)

	if errors > 0 {
		fmt.Printf("\n❌ TEST FAILED: %d errors occurred\n", errors)
		os.Exit(1)
	}

	// Check file size
	cfg := config.GetConfig()
	filePath := filepath.Join(cfg.DATA_DIR, "active.log")
	stat, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("Warning: Could not stat log file: %v\n", err)
	} else {
		fmt.Printf("Log file size: %d bytes (%.2f MB)\n", stat.Size(), float64(stat.Size())/1024/1024)
	}

	// Check keyDir size
	keyDirSize := kv.GetKeyDirSize()
	fmt.Printf("Keys in memory (keyDir): %d\n", keyDirSize)
	if keyDirSize != totalKeys {
		fmt.Printf("⚠️  WARNING: keyDir has %d keys, expected %d\n", keyDirSize, totalKeys)
	}

	if elapsed > 2*time.Minute {
		fmt.Printf("\n⚠️  WARNING: Write took over 2 minutes (%.2f minutes)\n", elapsed.Minutes())
		fmt.Println("   This is expected with os.O_APPEND and Sync() - every write waits for disk.")
		fmt.Println("   Real databases use 'Group Commit' or 'Batching' to improve performance.")
	}

	fmt.Println("\n✅ TEST PASSED: All 100,000 keys written successfully")
}

// Test 2: Overlapping Key Test
func testOverlappingKey() {
	fmt.Println("=" + strings.Repeat("=", 60))
	fmt.Println("Test 2: Overlapping Key Test")
	fmt.Println("=" + strings.Repeat("=", 60))

	kv, err := engine.NewKVEngine()
	if err != nil {
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer kv.Close()

	key := "key_1"
	valueA := "value_A"
	valueB := "value_B"

	// Get initial file size
	cfg := config.GetConfig()
	filePath := filepath.Join(cfg.DATA_DIR, "active.log")
	initialStat, _ := os.Stat(filePath)
	initialSize := int64(0)
	if initialStat != nil {
		initialSize = initialStat.Size()
	}

	fmt.Printf("Step 1: Putting %s with value '%s'\n", key, valueA)
	if err := kv.Put(key, valueA); err != nil {
		log.Fatalf("Failed to put key_1 with value_A: %v", err)
	}

	afterFirstStat, _ := os.Stat(filePath)
	firstSize := int64(0)
	if afterFirstStat != nil {
		firstSize = afterFirstStat.Size()
	}
	fmt.Printf("  Log file size after first write: %d bytes\n", firstSize)

	fmt.Printf("\nStep 2: Putting %s with value '%s' (overwriting)\n", key, valueB)
	if err := kv.Put(key, valueB); err != nil {
		log.Fatalf("Failed to put key_1 with value_B: %v", err)
	}

	afterSecondStat, _ := os.Stat(filePath)
	secondSize := int64(0)
	if afterSecondStat != nil {
		secondSize = afterSecondStat.Size()
	}
	fmt.Printf("  Log file size after second write: %d bytes\n", secondSize)
	fmt.Printf("  Log file grew by: %d bytes (should be > 0, contains both versions)\n", secondSize-initialSize)

	fmt.Printf("\nStep 3: Getting %s\n", key)
	value, err := kv.Get(key)
	if err != nil {
		log.Fatalf("Failed to get key_1: %v", err)
	}

	fmt.Printf("  Retrieved value: '%s'\n", value)

	if value != valueB {
		fmt.Printf("\n❌ TEST FAILED: Expected '%s', got '%s'\n", valueB, value)
		os.Exit(1)
	}

	// Check keyDir
	keyDirSize := kv.GetKeyDirSize()
	if keyDirSize != 1 {
		fmt.Printf("⚠️  WARNING: keyDir has %d keys, expected 1\n", keyDirSize)
	} else {
		fmt.Printf("  keyDir contains 1 key (correct - only latest offset)\n")
	}

	fmt.Println("\n✅ TEST PASSED: Latest value correctly returned")
}

// Test 3: Integrity Test (Read-Back)
func testIntegrity() {
	fmt.Println("=" + strings.Repeat("=", 60))
	fmt.Println("Test 3: Integrity Test (Read-Back)")
	fmt.Println("=" + strings.Repeat("=", 60))

	kv, err := engine.NewKVEngine()
	if err != nil {
		log.Fatalf("Failed to create KV engine: %v", err)
	}
	defer kv.Close()

	totalKeys := 100000
	fmt.Printf("Step 1: Writing %d keys...\n", totalKeys)
	startTime := time.Now()

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		if err := kv.Put(key, value); err != nil {
			log.Fatalf("Failed to put key_%d: %v", i, err)
		}
	}

	writeTime := time.Since(startTime)
	fmt.Printf("  Write completed in %v\n", writeTime)

	// Randomly pick 1,000 keys
	fmt.Printf("\nStep 2: Randomly reading 1,000 keys to verify integrity...\n")
	rand.Seed(time.Now().UnixNano())
	readStartTime := time.Now()
	errors := 0
	crcErrors := 0

	for i := 0; i < 1000; i++ {
		randomIndex := rand.Intn(totalKeys)
		key := fmt.Sprintf("key_%d", randomIndex)
		expectedValue := fmt.Sprintf("value_%d", randomIndex)

		value, err := kv.Get(key)
		if err != nil {
			errors++
			if errors <= 10 {
				fmt.Printf("  ERROR: Failed to get %s: %v\n", key, err)
				if strings.Contains(err.Error(), "crc mismatch") {
					crcErrors++
					fmt.Printf("    ⚠️  CRC MISMATCH - offset calculation may be wrong!\n")
				}
			}
			continue
		}

		if value != expectedValue {
			errors++
			if errors <= 10 {
				fmt.Printf("  ERROR: Value mismatch for %s\n", key)
				fmt.Printf("    Expected: '%s'\n", expectedValue)
				fmt.Printf("    Got:      '%s'\n", value)
			}
		}
	}

	readTime := time.Since(readStartTime)
	fmt.Printf("\n  Read completed in %v\n", readTime)
	fmt.Printf("  Read rate: %.2f keys/second\n", 1000.0/readTime.Seconds())

	fmt.Println("\n" + strings.Repeat("-", 60))
	fmt.Printf("Errors: %d\n", errors)
	if crcErrors > 0 {
		fmt.Printf("CRC Mismatches: %d\n", crcErrors)
		fmt.Println("\n⚠️  CRC MISMATCH DETECTED!")
		fmt.Println("   This indicates your offset calculation in engine.go is wrong.")
		fmt.Println("   You're likely pointing to the middle of a record instead of the start.")
	}

	if errors > 0 {
		fmt.Printf("\n❌ TEST FAILED: %d errors occurred\n", errors)
		os.Exit(1)
	}

	fmt.Println("\n✅ TEST PASSED: All 1,000 random reads returned correct values")
}
