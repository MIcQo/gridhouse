#!/bin/bash

echo "=== Detailed Benchmark Replication Analysis ==="

# Clean up
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level info --port :6380 --rdb --dir ./data/master > master_bench_detailed.log 2>&1 &
MASTER_PID=$!
sleep 2

# Start slave
echo "2. Starting slave server..."
./gridhouse --log-level info --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave_bench_detailed.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Run smaller benchmark to track commands
echo "3. Running small benchmark (100 requests)..."
redis-benchmark -p 6380 -n 100 -q -c 1 > benchmark_small.txt 2>&1
sleep 3

# Check database sizes
echo "4. Checking database sizes..."
MASTER_DBSIZE=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6380 | tail -1 | cut -c2-)
SLAVE_DBSIZE=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6381 | tail -1 | cut -c2-)

echo "   Master DBSIZE: $MASTER_DBSIZE"
echo "   Slave DBSIZE: $SLAVE_DBSIZE"
echo "   Difference: $((MASTER_DBSIZE - SLAVE_DBSIZE)) keys"

# Check what commands were forwarded
echo "5. Commands forwarded by master:"
grep -c "Successfully forwarded" master_bench_detailed.log || echo "0"

# Check what commands were received by slave
echo "6. Commands received by slave:"
grep -c "Received command from master" slave_bench_detailed.log || echo "0"

# Check for connection issues
echo "7. Checking for connection issues..."
echo "   Master connection errors:"
grep -c -i "failed.*replica\|connection.*error" master_bench_detailed.log || echo "0"
echo "   Slave connection errors:"
grep -c -i "failed.*command\|connection.*error" slave_bench_detailed.log || echo "0"

# Check specific command types in benchmark
echo "8. Benchmark command breakdown:"
cat benchmark_small.txt

# Sample some keys to see what's missing
echo "9. Sampling keys on master vs slave..."
echo "   First 5 keys on master:"
echo -e "*3\r\n\$4\r\nKEYS\r\n\$1\r\n*\r\n" | nc localhost 6380 | head -10
echo "   First 5 keys on slave:"
echo -e "*3\r\n\$4\r\nKEYS\r\n\$1\r\n*\r\n" | nc localhost 6381 | head -10

# Stop servers
echo "10. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo "=== Detailed Analysis Complete ==="
