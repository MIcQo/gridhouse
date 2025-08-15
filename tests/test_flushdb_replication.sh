#!/bin/bash

echo "=== Testing FLUSHDB Replication ==="

# Clean up
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level debug --port :6380 --rdb --dir ./data/master > master_flush.log 2>&1 &
MASTER_PID=$!
sleep 2

# Start slave
echo "2. Starting slave server..."
./gridhouse --log-level debug --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave_flush.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Add some data
echo "3. Adding test data..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380
sleep 2

# Check initial state
echo "4. Checking initial database sizes..."
echo "   DBSIZE on master:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6380
echo "   DBSIZE on slave:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6381

# Execute FLUSHDB on master
echo "5. Executing FLUSHDB on master..."
echo -e "*1\r\n\$7\r\nFLUSHDB\r\n" | nc localhost 6380
sleep 5  # Wait longer for replication

# Check final state
echo "6. Checking database sizes after FLUSHDB (with delay)..."
echo "   DBSIZE on master:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6380
echo "   DBSIZE on slave:"
echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc localhost 6381

# Check logs for FLUSHDB forwarding
echo "7. Checking master logs for FLUSHDB forwarding..."
grep -E "(FLUSHDB|Forwarding.*FLUSHDB)" master_flush.log

echo "8. Checking slave logs for FLUSHDB reception..."
grep -E "(FLUSHDB|Received.*FLUSHDB|Replicated.*FLUSHDB)" slave_flush.log

# Stop servers
echo "9. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo "=== FLUSHDB Replication Test Complete ==="
