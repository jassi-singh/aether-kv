# Aether KV Test Suite

This directory contains comprehensive tests for the Aether KV key-value store engine.

## Running Tests

All tests can be run from the project root:

```bash
go run tests/test.go <test-name>
```

## Available Tests

### 1. 100k Write Test (Speed & Integrity)

**Purpose**: Measure write performance and ensure no errors occur during bulk writes.

**Command**:
```bash
go run tests/test.go 100k-write
```

**What it does**:
- Writes 100,000 unique keys (key_0 to key_99999)
- Measures total time and write rate
- Checks for errors
- Verifies log file size
- Validates keyDir size

**Expected behavior**:
- All writes should succeed (0 errors)
- May take over 2 minutes (expected with `os.O_APPEND` and `Sync()`)
- Log file should grow significantly
- keyDir should contain exactly 100,000 entries

**Mentor Tip**: Since you're using `os.O_APPEND` and `Sync()`, this will be slow because every write waits for the physical disk platter to spin. If it's too slow (over 2 minutes), you'll understand why real databases like Postgres use "Group Commit" or "Batching."

---

### 2. Overlapping Key Test

**Purpose**: Verify that overwriting a key updates the value correctly.

**Command**:
```bash
go run tests/test.go overlapping
```

**What it does**:
- Puts `key_1` with value `value_A`
- Immediately puts `key_1` with value `value_B` (overwriting)
- Gets `key_1` and verifies it returns `value_B`
- Checks log file size (should contain both versions)
- Verifies keyDir points only to the latest offset

**Expected behavior**:
- `Get("key_1")` must return `value_B` (latest value)
- Log file should have grown twice (contains both versions)
- keyDir should contain only 1 key (pointing to the latest offset)

---

### 3. Integrity Test (Read-Back)

**Purpose**: Verify data integrity by randomly reading back written keys.

**Command**:
```bash
go run tests/test.go integrity
```

**What it does**:
- Writes 100,000 keys
- Randomly picks 1,000 keys and performs `Get` operations
- Verifies all values match exactly

**Expected behavior**:
- All 1,000 random reads should return correct values
- 0 errors expected
- If you get CRC Mismatch errors, your offset calculation in `engine.go` is wrong—you're likely pointing to the middle of a record instead of the start

**Troubleshooting**:
- **CRC Mismatch**: This indicates your offset calculation is incorrect. You're likely reading from the middle of a record instead of the start.

---

## Test Results Interpretation

### Success Indicators
- ✅ All tests pass with 0 errors
- ✅ Values match expected results
- ✅ File sizes are as expected
- ✅ keyDir contains correct number of entries

### Warning Indicators
- ⚠️ Write performance is slow (>2 minutes for 100k writes) - expected with current implementation
- ⚠️ CRC mismatches - indicates offset calculation issues
- ⚠️ keyDir size mismatch - indicates tracking issues

### Failure Indicators
- ❌ Errors during writes
- ❌ Value mismatches during reads
- ❌ CRC mismatches (data corruption)
- ❌ Cannot retrieve keys after restart (recovery not implemented)

---

## Notes

- All tests use the same `active.log` file, so running multiple tests will append to the existing log
- The integrity test uses random sampling, so results may vary slightly
- Performance metrics are provided for benchmarking purposes

