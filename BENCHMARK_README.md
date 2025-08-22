# GridHouse Benchmark Command

The GridHouse benchmark command provides comprehensive Redis benchmarking capabilities similar to `redis-benchmark`, allowing you to test the performance of your GridHouse server.

## Features

- **Multiple Commands**: Test PING, SET, GET, INCR, LPUSH, RPUSH, LPOP, RPOP, SADD, HSET, SPOP, ZADD, ZPOPMIN, LRANGE, MSET
- **Concurrent Testing**: Multiple parallel connections for realistic load testing
- **Pipelining**: Support for pipelined requests to test high-throughput scenarios
- **Latency Analysis**: Detailed latency percentiles (p50, p95, p99) and histograms
- **Flexible Output**: Standard output, quiet mode, and CSV format
- **Configurable Data**: Customizable data sizes and random data generation
- **Connection Options**: Support for authentication, database selection, and TLS

## Usage

### Basic Usage

```bash
# Basic benchmark with default settings
./gridhouse benchmark

# Test specific commands
./gridhouse benchmark --commands PING,SET,GET

# Custom number of requests and concurrency
./gridhouse benchmark --requests 10000 --concurrency 50
```

### Command Options

#### Connection Options
- `--host`: Redis server host (default: 127.0.0.1)
- `--port`: Redis server port (default: 6380)
- `--password`: Redis server password
- `--db`: Redis database number (default: 0)
- `--tls`: Use TLS connection

#### Benchmark Configuration
- `--requests`: Total number of requests (default: 10000)
- `--concurrency`: Number of parallel connections (default: 50)
- `--pipeline`: Pipeline requests (default: 1)
- `--timeout`: Connection timeout (default: 5s)
- `--keep-alive`: Use keep-alive connections (default: true)

#### Test Configuration
- `--commands`: Comma-separated list of commands to test
- `--data-size`: Data size of SET/GET values in bytes (default: 2)
- `--key-pattern`: Key pattern for testing (default: key:__rand_int__)
- `--keyspace`: Keyspace size for random key generation (default: 1000000)
- `--random-data`: Use random data for values

#### Output Options
- `--quiet`: Quiet mode (only show summary)
- `--csv`: Output in CSV format
- `--latency-hist`: Show latency histogram

## Examples

### Basic Performance Test
```bash
./gridhouse benchmark --requests 1000 --concurrency 10 --commands PING,SET,GET
```

### High-Throughput Test with Pipelining
```bash
./gridhouse benchmark --requests 10000 --concurrency 20 --pipeline 10 --commands PING,SET
```

### Detailed Latency Analysis
```bash
./gridhouse benchmark --requests 1000 --concurrency 5 --commands PING --latency-hist
```

### CSV Output for Analysis
```bash
./gridhouse benchmark --requests 5000 --concurrency 10 --commands PING,SET,GET,INCR --csv > results.csv
```

### Testing Different Data Sizes
```bash
# Small data
./gridhouse benchmark --requests 1000 --data-size 2 --commands SET,GET --quiet

# Medium data
./gridhouse benchmark --requests 1000 --data-size 100 --commands SET,GET --quiet

# Large data
./gridhouse benchmark --requests 1000 --data-size 1000 --commands SET,GET --quiet
```

### Comprehensive Benchmark
```bash
./gridhouse benchmark \
  --requests 10000 \
  --concurrency 50 \
  --commands PING,SET,GET,INCR,LPUSH,RPUSH,LPOP,RPOP,SADD,HSET,SPOP,ZADD,ZPOPMIN,LRANGE,MSET \
  --data-size 100 \
  --latency-hist
```

## Output Format

### Standard Output
```
Redis Benchmark Tool
===================
Host: 127.0.0.1:6380
Requests: 1000
Concurrency: 10
Pipeline: 1
Commands: PING, SET, GET
Data size: 2 bytes
Key pattern: key:__rand_int__
Keyspace: 1000000

Testing PING...
Testing SET...
Testing GET...

Benchmark Results:
=================
PING: 50000.00 requests per second
  Duration: 20.000 ms
  Requests: 1000
  Errors: 0
  Latency percentiles:
    p50: 100.000 µs
    p95: 200.000 µs
    p99: 500.000 µs

Summary:
  Total requests: 3000
  Total errors: 0
  Error rate: 0.00%
  Average throughput: 50000.00 requests/second
```

### Quiet Mode
```
PING: 50000.00 requests per second, p50=100.000 µs
SET: 45000.00 requests per second, p50=120.000 µs
GET: 55000.00 requests per second, p50=80.000 µs
```

### CSV Format
```
Command,Requests,Errors,Duration,Throughput,P50,P95,P99
PING,1000,0,20.000 ms,50000.00,100.000 µs,200.000 µs,500.000 µs
SET,1000,0,22.000 ms,45454.55,120.000 µs,250.000 µs,600.000 µs
GET,1000,0,18.000 ms,55555.56,80.000 µs,150.000 µs,400.000 µs
```

## Supported Commands

The benchmark tool supports the following Redis commands:

- **PING**: Basic connectivity test
- **SET**: Set key-value pairs
- **GET**: Retrieve values
- **INCR**: Increment counters
- **LPUSH/RPUSH**: Push to lists
- **LPOP/RPOP**: Pop from lists
- **SADD**: Add to sets
- **HSET**: Set hash fields
- **SPOP**: Pop from sets
- **ZADD**: Add to sorted sets
- **ZPOPMIN**: Pop minimum from sorted sets
- **LRANGE**: Range queries on lists
- **MSET**: Multiple set operations

## Performance Considerations

### Concurrency
- Higher concurrency increases throughput but may also increase latency
- Optimal concurrency depends on your server's capabilities
- Start with 10-50 connections and adjust based on results

### Pipelining
- Pipelining can significantly improve throughput
- Use pipeline sizes of 10-50 for optimal performance
- Larger pipeline sizes may increase memory usage

### Data Size
- Smaller data sizes generally result in higher throughput
- Test with realistic data sizes for your use case
- Consider the impact of data size on network bandwidth

### Key Distribution
- The benchmark uses a keyspace to distribute keys
- Larger keyspace reduces key conflicts
- Use `--keyspace` to adjust the key distribution

## Troubleshooting

### Connection Issues
- Ensure the GridHouse server is running on the specified host and port
- Check firewall settings if testing across networks
- Verify authentication credentials if using password protection

### Performance Issues
- Monitor system resources (CPU, memory, network)
- Adjust concurrency and pipeline settings
- Consider server configuration optimizations

### Memory Usage
- Large pipeline sizes may increase memory usage
- Monitor memory consumption during benchmarks
- Reduce concurrency if memory becomes a constraint

## Integration with CI/CD

The benchmark command can be integrated into CI/CD pipelines:

```bash
# Example CI/CD script
#!/bin/bash
set -e

# Start GridHouse server
./gridhouse --port :6380 &
SERVER_PID=$!
sleep 3

# Run benchmark
./gridhouse benchmark \
  --requests 1000 \
  --concurrency 10 \
  --commands PING,SET,GET \
  --csv > benchmark_results.csv

# Check performance thresholds
PING_THROUGHPUT=$(grep "PING" benchmark_results.csv | cut -d',' -f5)
if (( $(echo "$PING_THROUGHPUT < 10000" | bc -l) )); then
    echo "Performance below threshold: $PING_THROUGHPUT requests/sec"
    exit 1
fi

# Cleanup
kill $SERVER_PID
```

## Comparison with redis-benchmark

The GridHouse benchmark command is designed to be compatible with `redis-benchmark`:

```bash
# GridHouse benchmark
./gridhouse benchmark --requests 1000 --concurrency 10 --commands PING --quiet

# Redis benchmark (equivalent)
redis-benchmark -p 6380 -n 1000 -c 10 -q ping
```

Key differences:
- GridHouse uses `--commands` instead of individual command flags
- GridHouse provides more detailed latency analysis
- GridHouse supports CSV output for easier analysis
- GridHouse includes latency histograms for detailed performance analysis
