#!/bin/bash

echo "=== Database State Investigation ==="

# Clean up
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level debug --port :6380 --rdb --dir ./data/master > master_debug.log 2>&1 &
MASTER_PID=$!
sleep 2

# Add data BEFORE slave connects
echo "2. Adding data to master..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
sleep 1

# Start slave
echo "3. Starting slave server..."
./gridhouse --log-level debug --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave_debug.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Test immediately after replication completes
echo "4. Testing GET immediately after replication..."
echo "   GET key1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381

# Wait a bit and test again
sleep 2
echo "5. Testing GET after waiting..."
echo "   GET key1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381

# Check if there are any background operations that might clear data
echo "6. Checking for background operations in logs..."
echo "=== Slave Log (last 20 lines) ==="
tail -20 slave_debug.log

# Stop servers
echo "7. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo "=== Investigation Complete ==="
