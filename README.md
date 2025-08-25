# GridHouse
[![codecov](https://codecov.io/github/MIcQo/gridhouse/graph/badge.svg?token=YBF6MA7FEH)](https://codecov.io/github/MIcQo/gridhouse)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/MIcQo/gridhouse/gotests.yml)

A high-performance Redis-compatible in-memory database written in Go.

## Features

- **Redis Protocol Compatibility**: Full RESP (Redis Serialization Protocol) support
- **High Performance**: Optimized for maximum throughput (~8.6M ops/sec)
- **Data Structures**: Strings, Lists, Sets, Hashes, Sorted Sets, and Streams
- **Persistence**: AOF (Append Only File) and RDB support with configurable sync modes
- **Replication**: Master-slave replication with PSYNC protocol
- **Transactions**: Full ACID-compliant transaction support with WATCH/UNWATCH
- **Statistics**: Comprehensive server statistics and monitoring
- **Authentication**: Optional password-based authentication
- **Command Support**: 80+ Redis commands implemented

## Performance

The Redis Clone achieves exceptional performance through optimizations:

- **SET**: ~8.6M operations/second
- **GET**: ~6.8M operations/second
- **INCR**: ~2.3M operations/second
- **LPUSH/RPUSH**: ~3.7M operations/second
- **MSET (10 keys)**: ~650K operations/second

This performance is achieved through:
- Lock-free command registry with zero-contention lookups
- Optimized RESP encoding with direct byte writes
- Connection reuse and buffer optimization
- Memory-efficient data structures
- Reduced allocations and GC pressure

## Quick Start

### Building

```bash
# Build the server
go build -o gridhouse main.go
```

### Running the Server

```bash
# Start server on default port 6380
./gridhouse

# Start server on custom port
./gridhouse --port :6379

# Start with persistence enabled
./gridhouse --aof --rdb --dir ./data

# Start with authentication
./gridhouse --requirepass mypassword

# Start as replica
./gridhouse --slaveof localhost:6379

# Start with custom buffer sizes
./gridhouse --read-buffer 512000 --write-buffer 512000

# Start with custom log level
./gridhouse --log-level debug
```

### Available Command Line Flags

#### Server Configuration
- `--port`: Server port (default: :6380)
- `--log-level`: Log level (debug, info, warn, error, fatal) (default: info)
- `--read-buffer`: Read buffer size in bytes (default: 256KB)
- `--write-buffer`: Write buffer size in bytes (default: 0)

#### Persistence Configuration
- `--dir`: Persistence directory (default: ./data)
- `--aof`: Enable AOF persistence
- `--aof-sync`: AOF sync mode (always, everysec, no) (default: everysec)
- `--aof-rewrite`: Enable AOF rewrite (default: true)
- `--aof-rewrite-growth-threshold`: AOF rewrite growth threshold in bytes (default: 64MB)
- `--aof-rewrite-min-size`: AOF rewrite minimum size in bytes (default: 32MB)
- `--aof-rewrite-percentage`: AOF rewrite percentage threshold (default: 100)
- `--rdb`: Enable RDB persistence
- `--save-interval`: RDB save interval in seconds (default: 300)
- `--min-changes`: Minimum changes before RDB save (default: 1)

#### Authentication & Replication
- `--requirepass`: Password for AUTH command
- `--slaveof`: Replicate from master (format: host:port)

### Benchmarking

```bash
# Run comprehensive benchmarks

# Use redis-benchmark against the server
redis-benchmark -p 6380 -t get,set,mget,mset,incr,lpush,rpush,lpop,rpop,llen,lrange -q -n 1000000 -P 1000
```

## Commands Supported

### Basic Commands

- `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`
- `TTL`, `PTTL`, `EXPIRE`, `INCR`, `DECR`
- `KEYS`, `MSET`, `MGET`, `FLUSHDB`, `DBSIZE`
- `GETRANGE` (alias `SUBSTR`), `TYPE`

### Data Structure Commands

#### Lists
- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`

#### Sets
- `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD`, `SPOP`

#### Hashes
- `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HINCRBY`, `HINCRBYFLOAT`

#### Sorted Sets
- `ZADD`, `ZREM`, `ZCARD`, `ZSCORE`, `ZRANGE`, `ZPOPMIN`

#### Streams
- `XADD`, `XLEN`, `XRANGE`, `XDEL`, `XTRIM`, `XREAD`

### Server Commands

- `INFO`, `CONFIG`, `AUTH`, `SAVE`, `BGSAVE`
- `MEMORY` (USAGE, STATS)

### Transaction Commands

- `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

### Replication Commands

- `PSYNC`, `REPLCONF`, `SYNC`, `ROLE`

### Scan Commands

- `SCAN`, `SSCAN`, `HSCAN`

## Architecture

```
gridhouse/
├── cmd/                    # Command line tools
│   ├── benchmark/         # Performance benchmarking tool
│   └── root.go           # Main server entry point with CLI flags
├── internal/              # Core implementation
│   ├── cmd/              # Command handlers and registry
│   ├── persistence/      # AOF and RDB persistence
│   ├── repl/             # Replication logic
│   ├── resp/             # RESP protocol implementation
│   ├── server/           # Server and connection handling
│   ├── stats/            # Statistics and monitoring
│   └── store/            # Data storage and structures
└── main.go               # Server entry point
```

## Performance Comparison

Comprehensive benchmark results comparing GridHouse against Redis using 
`redis-benchmark -t get,set,mget,mset,incr,lpush,rpush,lpop,rpop,llen,lrange -q -n 1000000 -P 1000`:

| Metric              | GridHouse   | Redis      | Improvement | P50 Latency (GridHouse) | P50 Latency (Redis) |
|---------------------|-------------|------------|-------------|------------------------|---------------------|
| **PING_INLINE**     | 7.94M       | 5.32M      | **1.49x**   | 5.479 ms               | 8.103 ms            |
| **PING_MBULK**      | 8.00M       | 7.58M      | **1.06x**   | 4.695 ms               | 5.559 ms            |
| **SET**             | 7.87M       | 4.08M      | **1.93x**   | 5.967 ms               | 5.367 ms            |
| **GET**             | 6.33M       | 5.10M      | **1.24x**   | 5.631 ms               | 5.719 ms            |
| **INCR**            | 2.23M       | 4.72M      | **0.47x**   | 21.823 ms              | 4.679 ms            |
| **LPUSH**           | 3.47M       | 3.41M      | **1.02x**   | 13.415 ms              | 8.871 ms            |
| **RPUSH**           | 3.39M       | 4.61M      | **0.74x**   | 14.167 ms              | 6.351 ms            |
| **LPOP**            | 3.06M       | 2.95M      | **1.04x**   | 15.887 ms              | 10.447 ms           |
| **RPOP**            | 3.24M       | 3.72M      | **0.87x**   | 14.983 ms              | 8.063 ms            |
| **SADD**            | 3.97M       | 4.52M      | **0.88x**   | 12.191 ms              | 4.823 ms            |
| **HSET**            | 2.82M       | 3.69M      | **0.76x**   | 17.215 ms              | 4.799 ms            |
| **SPOP**            | 4.88M       | 5.85M      | **0.83x**   | 9.727 ms               | 5.279 ms            |
| **ZADD**            | 2.79M       | 3.60M      | **0.78x**   | 17.583 ms              | 5.791 ms            |
| **ZPOPMIN**         | 5.13M       | 6.06M      | **0.85x**   | 9.295 ms               | 4.383 ms            |
| **MSET (10 keys)**  | 0.57M       | 0.85M      | **0.67x**   | 86.271 ms              | 4.695 ms            |
| **XADD**            | 2.14M       | 1.93M      | **1.11x**   | 21.999 ms              | 9.823 ms            |

### List Range Performance (LRANGE)

| Range Size          | GridHouse   | Redis      | Improvement | P50 Latency (GridHouse) | P50 Latency (Redis) |
|---------------------|-------------|------------|-------------|------------------------|---------------------|
| **LRANGE_100**      | 220.3K      | 238.3K     | **0.92x**   | 12.535 ms              | 7.407 ms            |
| **LRANGE_300**      | 77.5K       | 57.9K      | **1.34x**   | 26.703 ms              | 139.391 ms          |
| **LRANGE_500**      | 43.9K       | 34.0K      | **1.29x**   | 33.311 ms              | 237.951 ms          |
| **LRANGE_600**      | 38.8K       | 26.9K      | **1.44x**   | 34.719 ms              | 279.551 ms          |

### Performance Summary

**GridHouse Strengths:**
- **SET operations**: 1.93x faster than Redis
- **GET operations**: 1.24x faster than Redis  
- **PING operations**: 1.49x faster (inline), 1.06x faster (mbulk)
- **Large LRANGE operations**: 1.29-1.44x faster with significantly lower latency
- **XADD (Streams)**: 1.11x faster than Redis
- **LPOP operations**: 1.04x faster than Redis

**Areas for Improvement:**
- **INCR operations**: 0.47x (slower due to string parsing overhead)
- **MSET operations**: 0.67x (slower with higher latency due to batch processing)
- **Hash operations**: HSET 0.76x (slower due to hash table overhead)
- **Sorted Set operations**: ZADD 0.78x, ZPOPMIN 0.85x
- **Set operations**: SADD 0.88x, SPOP 0.83x

**Key Insights:**
- GridHouse excels at basic SET/GET operations and large list range queries
- Redis performs better on atomic increment operations, batch multi-key operations, and complex data structures
- GridHouse shows significantly better latency for large LRANGE operations (300-600 elements)
- GridHouse has better performance for stream operations (XADD)
- Redis maintains advantages in hash, set, and sorted set operations

## Development

### Running Tests

```bash
# Run all tests
make test

# Run tests with race detection
make test-race

# Run benchmarks
make bench

# Run linting
make lint

# Tidy dependencies
make tidy
```

### Project Structure

- **Test-Driven Development**: All new features include comprehensive tests
- **Performance Focus**: Continuous benchmarking and optimization
- **Redis Compatibility**: Full RESP protocol compliance
- **Production Ready**: Error handling, logging, and monitoring

## License

This project is licensed under the GPL-3.0 License (LICENCE).

## Contributing

1. Follow test-driven development (TDD)
2. Ensure all tests pass
3. Run benchmarks to verify performance
4. Follow Go best practices and conventions
5. Use the optimization workflow: optimize code → run tests → run linter → run redis-benchmark → repeat

