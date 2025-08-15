#!/bin/bash

echo "=== Testing Replication with Redis Benchmark ==="

# Clean up
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level info --port :6380 --rdb --dir ./data/master > master_bench.log 2>&1 &
MASTER_PID=$!
sleep 2

# Start slave
echo "2. Starting slave server..."
./gridhouse --log-level info --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave_bench.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Run redis-benchmark on master
echo "3. Running redis-benchmark on master (1000 requests)..."
redis-benchmark -p 6380 -n 1000 -q --csv > benchmark_results.txt 2>&1
sleep 2

# Check dbsize on both
echo "4. Checking database sizes..."
echo "   DBSIZE on master:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6380
echo "   DBSIZE on slave:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6381

# Test FLUSHDB on master
echo "5. Testing FLUSHDB on master..."
echo -e "*1\r\n\$7\r\nFLUSHDB\r\n" | nc localhost 6380
sleep 2

echo "6. Checking database sizes after FLUSHDB..."
echo "   DBSIZE on master:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6380
echo "   DBSIZE on slave:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6381

# Show benchmark results
echo "7. Benchmark results:"
head -5 benchmark_results.txt

# Stop servers
echo "8. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo "=== Benchmark Replication Test Complete ==="
