#!/bin/bash

echo "Testing Ultra-Fast List Commands..."

# Start server
go run main.go &
SERVER_PID=$!

# Wait for server to start
sleep 2

echo "Testing LPUSH..."
redis-cli -p 6380 lpush mylist a b c

echo "Testing RPUSH..."
redis-cli -p 6380 rpush mylist d e f

echo "Testing LLEN..."
redis-cli -p 6380 llen mylist

echo "Testing LRANGE..."
redis-cli -p 6380 lrange mylist 0 -1

echo "Testing LPOP..."
redis-cli -p 6380 lpop mylist

echo "Testing RPOP..."
redis-cli -p 6380 rpop mylist

echo "Testing final state..."
redis-cli -p 6380 lrange mylist 0 -1

# Stop server
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Ultra-fast list commands test completed!"
