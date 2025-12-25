# Aether KV

Aether KV is a Log-Structured Hash Table (LSHT) key-value store implementation, similar to Bitcask. It provides a simple, efficient, and persistent key-value storage solution with append-only log files and in-memory indexing.

## Features

- **Log-Structured Storage**: All writes are append-only, providing excellent write performance
- **In-Memory Index**: Fast lookups using an in-memory key directory (keyDir) implemented with `sync.Map`
- **Thread-Safe**: Concurrent operations are supported with proper synchronization
- **Data Integrity**: CRC32 checksums for data corruption detection
- **Tombstone Support**: Efficient deletion using tombstone markers
- **Automatic Recovery**: Key directory is rebuilt from log file on startup
- **Buffered Writes**: Configurable batch size and sync intervals for performance tuning

## Architecture

### Components

- **Engine** (`internal/engine`): Core key-value storage engine with in-memory key directory
- **Storage** (`internal/storage`): File I/O operations with buffered writes and automatic flushing
- **Format** (`internal/format`): Binary record encoding/decoding with CRC validation
- **CLI** (`internal/cli`): Command-line interface for interactive usage
- **Config** (`internal/config`): Configuration management with YAML and environment variable support

### Design Decisions

- **sync.Map**: Used for the key directory to provide thread-safe concurrent access without explicit locking
- **Dependency Injection**: Configuration is injected rather than accessed globally, improving testability
- **Separation of Concerns**: Clear separation between engine logic, storage operations, and CLI handling
- **Error Wrapping**: Consistent error handling with proper error wrapping using `fmt.Errorf` with `%w`

## Project Structure

```
aether-kv/
├── cmd/
│   └── main.go              # Application entry point
├── internal/
│   ├── cli/
│   │   └── handler.go       # CLI command parsing and execution
│   ├── config/
│   │   ├── config.go        # Configuration loading and management
│   │   └── config.yml       # Configuration template
│   ├── engine/
│   │   ├── engine.go        # Core KV engine with key directory
│   │   └── engine_test.go   # Engine unit tests
│   ├── format/
│   │   ├── codec.go         # Record encoding/decoding
│   │   └── codec_test.go    # Format unit tests
│   └── storage/
│       ├── file.go          # File operations with buffering
│       └── file_test.go     # Storage unit tests
├── tests/
│   └── test.go              # Integration tests
├── data/
│   └── active.log           # Active log file (created at runtime)
├── go.mod
├── go.sum
└── Readme.md
```

## Building and Running

### Prerequisites

- Go 1.25.1 or later

### Build

```bash
go build -o aether-kv ./cmd/main.go
```

### Run

```bash
./aether-kv
```

Or directly:

```bash
go run ./cmd/main.go
```

### Usage

Once running, you can use the following commands:

- `PUT <key> <value>` - Store a key-value pair
- `GET <key>` - Retrieve the value for a key
- `DELETE <key>` - Delete a key (writes tombstone marker)
- `EXIT` or `QUIT` - Exit the application

Example:

```
> PUT user:1 "John Doe"
OK
> GET user:1
John Doe
> DELETE user:1
OK
> GET user:1
Error: key not found
> EXIT
Goodbye!
```

## Configuration

Configuration is managed through `internal/config/config.yml` and environment variables.

### Configuration File

Create `internal/config/config.yml`:

```yaml
DATA_DIR: ${DATA_DIR:-./data}
HEADER_SIZE: ${HEADER_SIZE:-21}
BATCH_SIZE: ${BATCH_SIZE:-4096}
SYNC_INTERVAL: ${SYNC_INTERVAL:-5}
```

### Environment Variables

You can override configuration using environment variables:

```bash
export DATA_DIR=/path/to/data
export HEADER_SIZE=21
export BATCH_SIZE=8192
export SYNC_INTERVAL=10
```

### Configuration Parameters

- **DATA_DIR**: Directory where log files are stored (default: `./data`)
- **HEADER_SIZE**: Size of record header in bytes (default: `21`)
- **BATCH_SIZE**: Buffer size threshold for auto-flush in bytes (default: `4096`)
- **SYNC_INTERVAL**: Time interval in seconds for auto-sync (default: `5`)

## Testing

### Unit Tests

Run all unit tests:

```bash
go test ./...
```

Run tests for a specific package:

```bash
go test ./internal/engine -v
go test ./internal/storage -v
go test ./internal/format -v
```

### Integration Tests

Run integration tests:

```bash
go run tests/test.go 100k-write
go run tests/test.go overlapping
go run tests/test.go integrity
```

## Record Format

Each record in the log file has the following binary format:

```
[0:4]   - CRC32 checksum (uint32, little-endian)
[4:12]  - Timestamp (uint64, little-endian)
[12:16] - Key size (uint32, little-endian)
[16:20] - Value size (uint32, little-endian)
[20:21] - Flag (uint8: 0=normal, 1=tombstone)
[21:]   - Key bytes followed by Value bytes
```

## Thread Safety

- **Key Directory**: Uses `sync.Map` for thread-safe concurrent access
- **File Operations**: All file operations (Append, ReadAt, Flush, Close) are protected by mutex
- **Engine Operations**: Get, Put, and Delete operations are safe for concurrent use

## Performance Considerations

- **Write Performance**: Append-only writes provide excellent write throughput
- **Read Performance**: In-memory key directory enables O(1) lookups
- **Buffering**: Configurable batch size allows tuning between latency and throughput
- **Sync Interval**: Automatic syncing ensures data durability while maintaining performance

## Limitations

- Single-file implementation (no file rotation or compaction yet)
- In-memory key directory (memory usage scales with number of keys)
- No transaction support
- No replication or distributed features

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
