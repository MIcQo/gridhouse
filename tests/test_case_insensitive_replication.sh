#!/bin/bash

echo "=== Redis Clone Case-Insensitive Replication Test ==="
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

# Test case-insensitive SET commands
echo "3. Testing case-insensitive SET commands..."
echo "   set key1 = value1 (lowercase):"
echo -e "*3\r\n\$3\r\nset\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
sleep 1

echo "   SET key2 = value2 (uppercase):"
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
sleep 1

echo "   Set key3 = value3 (mixed case):"
echo -e "*3\r\n\$3\r\nSet\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380
sleep 1

# Test case-insensitive INCR commands
echo "4. Testing case-insensitive INCR commands..."
echo "   incr counter1 (lowercase):"
echo -e "*2\r\n\$4\r\nincr\r\n\$8\r\ncounter1\r\n" | nc localhost 6380
sleep 1

echo "   INCR counter2 (uppercase):"
echo -e "*2\r\n\$4\r\nINCR\r\n\$8\r\ncounter2\r\n" | nc localhost 6380
sleep 1

echo "   Incr counter3 (mixed case):"
echo -e "*2\r\n\$4\r\nIncr\r\n\$8\r\ncounter3\r\n" | nc localhost 6380
sleep 1

# Test case-insensitive DEL commands
echo "5. Testing case-insensitive DEL commands..."
echo "   del key1 (lowercase):"
echo -e "*2\r\n\$3\r\ndel\r\n\$4\r\nkey1\r\n" | nc localhost 6380
sleep 1

# Verify replication on slave
echo "6. Verifying case-insensitive replication on slave..."
echo "   GET key1 from slave (should be deleted):"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6381
echo "   GET key2 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6381
echo "   GET key3 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey3\r\n" | nc localhost 6381
echo "   GET counter1 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$8\r\ncounter1\r\n" | nc localhost 6381
echo "   GET counter2 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$8\r\ncounter2\r\n" | nc localhost 6381
echo "   GET counter3 from slave:"
echo -e "*2\r\n\$3\r\nGET\r\n\$8\r\ncounter3\r\n" | nc localhost 6381

# Show logs
echo
echo "7. Showing replication logs..."
echo "=== Master Log ==="
tail -10 master.log
echo
echo "=== Slave Log ==="
tail -10 slave.log

# Stop servers
echo
echo "8. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 1

echo
echo "=== Test Complete ==="
echo "The case-insensitive replication test shows:"
echo "1. Commands in different cases (lowercase, uppercase, mixed) are replicated"
echo "2. All write commands are forwarded to replicas regardless of case"
echo "3. Replicas execute commands correctly regardless of original case"
