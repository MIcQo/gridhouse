#!/bin/bash

# Test script for Redis transaction functionality
# This script tests the basic transaction commands

echo "Testing Redis Transaction Functionality"
echo "======================================"

# Start the server in the background
echo "Starting gridhouse server..."
go run main.go &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo ""
echo "Testing MULTI/EXEC transaction:"
echo "MULTI" | nc localhost 6380
echo "SET key1 value1" | nc localhost 6380
echo "GET key1" | nc localhost 6380
echo "EXEC" | nc localhost 6380
echo ""

# Test basic transaction
echo "MULTI" | nc localhost 6380
echo "SET key1 value1" | nc localhost 6380
echo "GET key1" | nc localhost 6380
echo "EXEC" | nc localhost 6380

echo ""
echo "Testing transaction with DISCARD:" 
echo "MULTI" | nc localhost 6380
echo "SET key2 value2" | nc localhost 6380
echo "DISCARD" | nc localhost 6380
echo "GET key2" | nc localhost 6380
echo ""

# Test DISCARD
echo "MULTI" | nc localhost 6380
echo "SET key2 value2" | nc localhost 6380
echo "DISCARD" | nc localhost 6380
echo "GET key2" | nc localhost 6380

echo ""
echo "Testing WATCH functionality:"
echo "WATCH counter" | nc localhost 6380
echo "MULTI" | nc localhost 6380
echo "GET counter" | nc localhost 6380
echo "SET counter 1" | nc localhost 6380
echo "EXEC"  | nc localhost 6380
echo ""

# Test WATCH
echo "WATCH counter" | nc localhost 6380
echo "MULTI" | nc localhost 6380
echo "GET counter" | nc localhost 6380
echo "SET counter 1" | nc localhost 6380
echo "EXEC" | nc localhost 6380

echo ""
echo "Testing UNWATCH:"
echo "UNWATCH" | nc localhost 6380
echo ""

# Test UNWATCH
echo "UNWATCH" | nc localhost 6380

echo ""
echo "Transaction tests completed!"

# Stop the server
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Server stopped."

