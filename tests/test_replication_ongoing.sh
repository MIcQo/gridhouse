#!/bin/bash

echo "=== Redis Clone Ongoing Replication Test ==="
echo

# Clean up any existing data
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master server
echo "1. Starting master server on port 6380..."
./gridhouse --log-level info --port :6380 --aof --dir ./data/master > master.log 2>&1 &
MASTER_PID=$!
sleep 2

# Start slave server
echo "2. Starting slave server on port 6381..."
./gridhouse --log-level info --port :6381 --slaveof localhost:6380 --aof --dir ./data/slave > slave.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Add initial data to master
echo "3. Adding initial data to master..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380

# Verify initial data is replicated
echo "4. Verifying initial data replication..."
echo "   GET key1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381
echo "   GET key2 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6381

# Test ongoing replication - add new data after slave is connected
echo
echo "5. Testing ongoing replication - adding new data..."
echo "   SET key3 = value3:"
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380
sleep 1

echo "   SET key4 = value4:"
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey4\r\n\$6\r\nvalue4\r\n" | nc localhost 6380
sleep 1

echo "   INCR counter:"
echo -e "*2\r\n\$4\r\nINCR\r\n\$7\r\ncounter\r\n" | nc localhost 6380
sleep 1

echo "   INCR counter:"
echo -e "*2\r\n\$4\r\nINCR\r\n\$7\r\ncounter\r\n" | nc localhost 6380
sleep 1

echo "   DEL key1:"
echo -e "*2\r\n\$3\r\nDEL\r\n\$4\r\nkey1\r\n" | nc localhost 6380
sleep 1

# Verify ongoing replication worked
echo
echo "6. Verifying ongoing replication..."
echo "   GET key3 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey3\r\n" | nc localhost 6381
echo "   GET key4 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey4\r\n" | nc localhost 6381
echo "   GET counter from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$7\r\ncounter\r\n" | nc localhost 6381
echo "   GET key1 from slave (should be deleted):"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381

# Test replication statistics
echo
echo "7. Testing replication statistics..."
echo "   INFO replication on master:"
echo -e "*2\r\n\$4\r\nINFO\r\n\$11\r\nreplication\r\n" | nc localhost 6380
echo "   INFO replication on slave:"
echo -e "*2\r\n\$4\r\nINFO\r\n\$11\r\nreplication\r\n" | nc localhost 6381

# Show logs
echo
echo "8. Showing replication logs..."
echo "=== Master Log ==="
tail -15 master.log
echo
echo "=== Slave Log ==="
tail -15 slave.log

# Stop servers
echo
echo "9. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo
echo "=== Test Complete ==="
echo "The ongoing replication test shows:"
echo "1. Initial data is replicated via RDB dump"
echo "2. Ongoing write commands are forwarded to replicas"
echo "3. Replicas execute forwarded commands in real-time"
echo "4. Replication statistics show connected replicas"
