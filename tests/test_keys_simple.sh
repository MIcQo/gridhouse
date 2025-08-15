#!/bin/bash

echo "Testing Redis Clone KEYS Command..."

# Start server on port 6380
echo "Starting server on port 6380..."
./gridhouse --port :6380 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test KEYS command
echo "Testing KEYS command..."

# Add some test data
echo "SET user:1 alice" | nc localhost 6380
echo "SET user:2 bob" | nc localhost 6380
echo "SET session:abc active" | nc localhost 6380

# Test KEYS with no pattern
echo "KEYS" | nc localhost 6380

# Test KEYS with wildcard pattern
echo "KEYS *" | nc localhost 6380

# Test KEYS with specific pattern
echo "KEYS user:*" | nc localhost 6380

echo "KEYS command test completed!"

# Clean up
echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
