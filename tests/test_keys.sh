#!/bin/bash

echo "Testing Redis Clone KEYS Command..."

# Start server
echo "Starting server..."
./gridhouse --port :6379 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test KEYS command
echo "Testing KEYS command..."

# Add some test data
echo "SET user:1 alice" | nc localhost 6379
echo "SET user:2 bob" | nc localhost 6379
echo "SET session:abc active" | nc localhost 6379
echo "SET config:redis enabled" | nc localhost 6379
echo "SET temp:123 data" | nc localhost 6379

# Test KEYS with no pattern (should return all keys)
echo "KEYS" | nc localhost 6379

# Test KEYS with wildcard pattern
echo "KEYS *" | nc localhost 6379

# Test KEYS with specific patterns
echo "KEYS user:*" | nc localhost 6379
echo "KEYS session:*" | nc localhost 6379
echo "KEYS config:*" | nc localhost 6379
echo "KEYS temp:*" | nc localhost 6379

# Test KEYS with non-matching pattern
echo "KEYS nonexistent:*" | nc localhost 6379

# Test KEYS with exact match
echo "KEYS user:1" | nc localhost 6379

# Test KEYS with complex patterns
echo "SET user:1:profile data1" | nc localhost 6379
echo "SET user:2:profile data2" | nc localhost 6379
echo "SET user:3:settings data3" | nc localhost 6379

echo "KEYS user:*:profile" | nc localhost 6379
echo "KEYS user:*:settings" | nc localhost 6379

# Test KEYS with data structures
echo "LPUSH mylist a b c" | nc localhost 6379
echo "SADD myset x y z" | nc localhost 6379
echo "HSET myhash field1 value1" | nc localhost 6379

echo "KEYS my*" | nc localhost 6379

# Test KEYS with special characters
echo "SET key-with-dashes value1" | nc localhost 6379
echo "SET key_with_underscores value2" | nc localhost 6379
echo "SET key.with.dots value3" | nc localhost 6379
echo "SET key:with:colons value4" | nc localhost 6379

echo "KEYS key-with-*" | nc localhost 6379
echo "KEYS key_with_*" | nc localhost 6379
echo "KEYS key.with.*" | nc localhost 6379
echo "KEYS key:with:*" | nc localhost 6379

# Test KEYS on empty database (after clearing)
echo "FLUSHDB" | nc localhost 6379
echo "KEYS *" | nc localhost 6379

echo "KEYS command test completed!"

# Clean up
echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
