#!/bin/bash

echo "Testing Redis Clone Comprehensive Statistics..."

# Start server
echo "Starting server..."
./gridhouse --port :6380 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test basic INFO command
echo "Testing INFO command..."
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc localhost 6380

echo ""
echo "Testing INFO server section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$6\r\nserver\r\n" | nc localhost 6380

echo ""
echo "Testing INFO clients section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$7\r\nclients\r\n" | nc localhost 6380

echo ""
echo "Testing INFO memory section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$6\r\nmemory\r\n" | nc localhost 6380

echo ""
echo "Testing INFO stats section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$5\r\nstats\r\n" | nc localhost 6380

echo ""
echo "Testing INFO commands section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$8\r\ncommands\r\n" | nc localhost 6380

echo ""
echo "Testing INFO keyspace section..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$8\r\nkeyspace\r\n" | nc localhost 6380

# Generate some activity to populate stats
echo ""
echo "Generating activity to populate stats..."

# Set some keys
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc localhost 6380
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc localhost 6380

# Get some keys
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey1\r\n" | nc localhost 6380
echo -e "*2\r\n\$3\r\nGET\r\n\$4\r\nkey2\r\n" | nc localhost 6380
echo -e "*2\r\n\$3\r\nGET\r\n\$11\r\nnonexistent\r\n" | nc localhost 6380

# Use data structures
echo -e "*4\r\n\$5\r\nLPUSH\r\n\$6\r\nmylist\r\n\$1\r\na\r\n\$1\r\nb\r\n" | nc localhost 6380
echo -e "*4\r\n\$4\r\nSADD\r\n\$5\r\nmyset\r\n\$1\r\nx\r\n\$1\r\ny\r\n" | nc localhost 6380
echo -e "*4\r\n\$4\r\nHSET\r\n\$6\r\nmyhash\r\n\$6\r\nfield1\r\n\$6\r\nvalue1\r\n" | nc localhost 6380

# Use MSET/MGET
echo -e "*6\r\n\$4\r\nMSET\r\n\$5\r\nuser1\r\n\$5\r\nalice\r\n\$5\r\nuser2\r\n\$3\r\nbob\r\n" | nc localhost 6380
echo -e "*4\r\n\$4\r\nMGET\r\n\$5\r\nuser1\r\n\$5\r\nuser2\r\n\$5\r\nuser3\r\n" | nc localhost 6380

# Test KEYS command
echo -e "*2\r\n\$4\r\nKEYS\r\n\$1\r\n*\r\n" | nc localhost 6380

# Test with expiration
echo -e "*5\r\n\$3\r\nSET\r\n\$7\r\ntempkey\r\n\$9\r\ntempvalue\r\n\$2\r\nEX\r\n\$1\r\n1\r\n" | nc localhost 6380
sleep 2

# Now check stats again
echo ""
echo "Testing INFO after activity..."
echo -e "*1\r\n\$4\r\nINFO\r\n" | nc localhost 6380

echo ""
echo "Testing INFO stats section after activity..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$5\r\nstats\r\n" | nc localhost 6380

echo ""
echo "Testing INFO commands section after activity..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$8\r\ncommands\r\n" | nc localhost 6380

echo ""
echo "Testing INFO keyspace section after activity..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$8\r\nkeyspace\r\n" | nc localhost 6380

# Test multiple connections
echo ""
echo "Testing multiple connections..."
for i in {1..3}; do
    echo -e "*1\r\n\$4\r\nPING\r\n" | nc localhost 6380
done

echo ""
echo "Testing INFO clients after multiple connections..."
echo -e "*2\r\n\$4\r\nINFO\r\n\$7\r\nclients\r\n" | nc localhost 6380

echo "Comprehensive statistics test completed!"

# Clean up
echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
