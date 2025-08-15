#!/bin/bash

echo "=== Redis Clone Replication Test (Fixed Order) ==="
echo

# Clean up any existing data
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master server
echo "1. Starting master server on port 6380..."
./gridhouse --log-level debug --port :6380 --rdb --dir ./data/master > master.log 2>&1 &
MASTER_PID=$!
sleep 2

# Add some data to master BEFORE starting slave
echo "2. Adding data to master BEFORE slave connects..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380
sleep 1

# Verify data is in master
echo "3. Verifying data in master..."
echo "   GET key1:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6380
echo "   GET key2:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6380
echo "   GET key3:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey3\r\n" | nc localhost 6380

# Start slave server AFTER data exists
echo
echo "4. Starting slave server on port 6381 AFTER data exists..."
./gridhouse --log-level debug --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Test replication
echo "5. Testing replication..."
echo "   GET key1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381
echo "   GET key2 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6381
echo "   GET key3 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey3\r\n" | nc localhost 6381

# Show logs
echo
echo "6. Showing replication logs..."
echo "=== Master Log ==="
tail -10 master.log
echo
echo "=== Slave Log ==="
tail -15 slave.log

# Stop servers
echo
echo "7. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo
echo "=== Test Complete ==="
