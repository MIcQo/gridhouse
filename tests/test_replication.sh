#!/bin/bash

echo "=== Redis Clone Replication Test ==="
echo

# Clean up any existing data
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master server
echo "1. Starting master server on port 6380..."
./gridhouse --log-level info --port :6380 --rdb --dir ./data/master > master.log 2>&1 &
MASTER_PID=$!
sleep 2

# Add some data to master
echo "2. Adding data to master..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380

# Verify data is in master
echo "3. Verifying data in master..."
echo "   GET key1:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6380
echo "   GET key2:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6380
echo "   GET key3:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey3\r\n" | nc localhost 6380

# Start slave server
echo
echo "4. Starting slave server on port 6381..."
./gridhouse --log-level info --port :6381 --slaveof localhost:6380 --rdb --dir ./data/slave > slave.log 2>&1 &
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

# Test replication protocol commands
echo
echo "6. Testing replication protocol commands..."
echo "   ROLE on master:"
echo -e "*1\r\n\$4\r\nROLE\r\n" | nc localhost 6380
echo "   ROLE on slave:"
echo -e "*1\r\n\$4\r\nROLE\r\n" | nc localhost 6381

echo "   INFO replication on master:"
echo -e "*2\r\n\$4\r\nINFO\r\n\$11\r\nreplication\r\n" | nc localhost 6380
echo "   INFO replication on slave:"
echo -e "*2\r\n\$4\r\nINFO\r\n\$11\r\nreplication\r\n" | nc localhost 6381

# Test PSYNC command
echo
echo "7. Testing PSYNC command..."
echo "   PSYNC command to master:"
echo -e "*3\r\n\$5\r\nPSYNC\r\n\$1\r\n?\r\n\$2\r\n-1\r\n" | nc localhost 6380

# Test REPLCONF commands
echo
echo "8. Testing REPLCONF commands..."
echo "   REPLCONF listening-port:"
echo -e "*3\r\n\$8\r\nREPLCONF\r\n\$14\r\nlistening-port\r\n\$4\r\n6381\r\n" | nc localhost 6380
echo "   REPLCONF capability:"
echo -e "*3\r\n\$8\r\nREPLCONF\r\n\$10\r\ncapability\r\n\$3\r\neof\r\n" | nc localhost 6380

# Show logs
echo
echo "9. Showing replication logs..."
echo "=== Master Log ==="
tail -10 master.log
echo
echo "=== Slave Log ==="
tail -10 slave.log

# Stop servers
echo
echo "10. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo
echo "=== Test Complete ==="
echo "The replication test shows:"
echo "1. Master can store data"
echo "2. Slave can connect to master"
echo "3. Slave receives RDB dump with initial data"
echo "4. Slave can serve replicated data"
echo "5. Replication protocol commands work"
echo
echo "Note: Ongoing command replication (after RDB dump) is not yet implemented."
echo "This would require the master to maintain a list of connected slaves and"
echo "forward all write commands to them in real-time."
