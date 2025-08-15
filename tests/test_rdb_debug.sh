#!/bin/bash

echo "=== RDB Debug Test ==="

# Clean up
rm -rf ./test_rdb_data
mkdir -p ./test_rdb_data

# Start master only
echo "1. Starting master server..."
./gridhouse --log-level debug --port :6380 --rdb --dir ./test_rdb_data > rdb_test.log 2>&1 &
MASTER_PID=$!
sleep 2

# Add data
echo "2. Adding test data..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\ntest\r\n\$5\r\nhello\r\n" | nc localhost 6380

# Force RDB save
echo "3. Forcing RDB save..."
echo -e "*1\r\n\$4\r\nSAVE\r\n" | nc localhost 6380

# Check if RDB file was created
echo "4. Checking RDB file..."
ls -la ./test_rdb_data/
if [ -f "./test_rdb_data/dump.rdb" ]; then
    echo "RDB file size: $(wc -c < ./test_rdb_data/dump.rdb) bytes"
    echo "RDB file hex dump (first 50 bytes):"
    hexdump -C ./test_rdb_data/dump.rdb | head -5
else
    echo "ERROR: No RDB file created!"
fi

# Stop master
echo "5. Stopping master..."
kill $MASTER_PID 2>/dev/null
sleep 1

echo "6. Master log:"
tail -10 rdb_test.log

echo "=== RDB Debug Test Complete ==="
