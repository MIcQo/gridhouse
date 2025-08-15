#!/bin/bash

# Test script for data structures
set -e

echo "Testing Redis Clone Data Structures..."

# Kill any existing gridhouse processes
pkill -f gridhouse 2>/dev/null || true
sleep 1

# Start the server
echo "Starting server..."
./gridhouse --port :6380 &
SERVER_PID=$!
sleep 2

# Test Lists
echo "Testing Lists..."
echo "LPUSH mylist a b c" | nc localhost 6380
echo "RPUSH mylist d e f" | nc localhost 6380
echo "LLEN mylist" | nc localhost 6380
echo "LRANGE mylist 0 -1" | nc localhost 6380
echo "LPOP mylist" | nc localhost 6380
echo "RPOP mylist" | nc localhost 6380
echo "LINDEX mylist 1" | nc localhost 6380
echo "LSET mylist 1 x" | nc localhost 6380
echo "LREM mylist 1 b" | nc localhost 6380
echo "LTRIM mylist 0 2" | nc localhost 6380

# Test Sets
echo "Testing Sets..."
echo "SADD myset a b c d" | nc localhost 6380
echo "SADD myset e f" | nc localhost 6380
echo "SCARD myset" | nc localhost 6380
echo "SMEMBERS myset" | nc localhost 6380
echo "SISMEMBER myset a" | nc localhost 6380
echo "SISMEMBER myset z" | nc localhost 6380
echo "SREM myset a c" | nc localhost 6380
echo "SPOP myset" | nc localhost 6380

# Test Hashes
echo "Testing Hashes..."
echo "HSET myhash field1 value1 field2 value2" | nc localhost 6380
echo "HSET myhash field3 value3" | nc localhost 6380
echo "HGET myhash field1" | nc localhost 6380
echo "HGET myhash field2" | nc localhost 6380
echo "HEXISTS myhash field1" | nc localhost 6380
echo "HEXISTS myhash field4" | nc localhost 6380
echo "HLEN myhash" | nc localhost 6380
echo "HKEYS myhash" | nc localhost 6380
echo "HVALS myhash" | nc localhost 6380
echo "HGETALL myhash" | nc localhost 6380
echo "HINCRBY myhash counter 5" | nc localhost 6380
echo "HINCRBY myhash counter 3" | nc localhost 6380
echo "HINCRBYFLOAT myhash float 1.5" | nc localhost 6380
echo "HINCRBYFLOAT myhash float 2.3" | nc localhost 6380
echo "HDEL myhash field1 field2" | nc localhost 6380

# Test case-insensitive commands
echo "Testing case-insensitive commands..."
echo "lpush MYLIST x y z" | nc localhost 6380
echo "sadd MYSET a b c" | nc localhost 6380
echo "hset MYHASH f1 v1 f2 v2" | nc localhost 6380

# Clean up
echo "Cleaning up..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true

echo "Data structures test completed!"
