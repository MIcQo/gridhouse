#!/bin/bash

echo "=== Testing Ongoing Command Replication ==="

# Clean up
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level debug --port :6380 --rdb --dir ./data/master > master_ongoing.log 2>&1 &
MASTER_PID=$!
sleep 2

# Add initial data BEFORE slave connects
echo "2. Adding initial data to master..."
echo -e "*3\r\n\$3\r\nSET\r\n\$7\r\ninitial\r\n\$5\r\nvalue\r\n" | nc localhost 6380
sleep 1

# Start slave
echo "3. Starting slave server..."
./gridhouse --log-level debug --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave_ongoing.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Test initial replication
echo "4. Testing initial replication..."
echo "   GET initial from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$7\r\ninitial\r\n" | nc localhost 6381

# Now test ONGOING replication - add data AFTER slave is connected
echo "5. Adding NEW data to master (after slave connected)..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nnew1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nnew2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
echo -e "*2\r\n\$4\r\nINCR\r\n\$7\r\ncounter\r\n" | nc localhost 6380
sleep 2

# Test if new data was replicated
echo "6. Testing ONGOING replication..."
echo "   GET new1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nnew1\r\n" | nc localhost 6381
echo "   GET new2 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nnew2\r\n" | nc localhost 6381
echo "   GET counter from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$7\r\ncounter\r\n" | nc localhost 6381

# Add more data to test continuous replication
echo "7. Testing continuous replication..."
echo -e "*2\r\n\$4\r\nINCR\r\n\$7\r\ncounter\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nnew3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380
sleep 2

echo "   GET counter from slave (should be 2):"
echo -e "*2\r\n\$3\r\nGET\r\n\$7\r\ncounter\r\n" | nc localhost 6381
echo "   GET new3 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nnew3\r\n" | nc localhost 6381

# Show replication logs
echo "8. Showing replication logs..."
echo "=== Master Log (last 15 lines) ==="
tail -15 master_ongoing.log | grep -E "(Forwarded|Registered|replica|PSYNC|RDB)"

echo "=== Slave Log (last 15 lines) ==="
tail -15 slave_ongoing.log | grep -E "(Received|command|string|Replicated)"

# Stop servers
echo "9. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo "=== Ongoing Replication Test Complete ==="
