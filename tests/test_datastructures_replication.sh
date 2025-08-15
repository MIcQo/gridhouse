#!/bin/bash

# Test script for data structures with replication
set -e

echo "Testing Redis Clone Data Structures with Replication..."

# Kill any existing gridhouse processes
pkill -f gridhouse 2>/dev/null || true
sleep 1

# Start master server
echo "Starting master server..."
./gridhouse --port :6381 &
MASTER_PID=$!
sleep 2

# Start slave server
echo "Starting slave server..."
./gridhouse --port :6382 --slaveof localhost:6381 &
SLAVE_PID=$!
sleep 3

# Test Lists on master
echo "Testing Lists on master..."
echo "LPUSH mylist a b c" | nc localhost 6381
echo "RPUSH mylist d e f" | nc localhost 6381
echo "LLEN mylist" | nc localhost 6381

# Verify on slave
echo "Verifying Lists on slave..."
echo "LLEN mylist" | nc localhost 6382
echo "LRANGE mylist 0 -1" | nc localhost 6382

# Test Sets on master
echo "Testing Sets on master..."
echo "SADD myset a b c d" | nc localhost 6381
echo "SCARD myset" | nc localhost 6381

# Verify on slave
echo "Verifying Sets on slave..."
echo "SCARD myset" | nc localhost 6382
echo "SMEMBERS myset" | nc localhost 6382

# Test Hashes on master
echo "Testing Hashes on master..."
echo "HSET myhash field1 value1 field2 value2" | nc localhost 6381
echo "HLEN myhash" | nc localhost 6381

# Verify on slave
echo "Verifying Hashes on slave..."
echo "HLEN myhash" | nc localhost 6382
echo "HGETALL myhash" | nc localhost 6382

# Test ongoing replication with data structures
echo "Testing ongoing replication with data structures..."
echo "LPUSH mylist2 x y z" | nc localhost 6381
echo "SADD myset2 p q r" | nc localhost 6381
echo "HSET myhash2 f1 v1 f2 v2" | nc localhost 6381

# Verify on slave
echo "Verifying ongoing replication on slave..."
echo "LLEN mylist2" | nc localhost 6382
echo "SCARD myset2" | nc localhost 6382
echo "HLEN myhash2" | nc localhost 6382

# Test case-insensitive replication
echo "Testing case-insensitive replication..."
echo "lpush MYLIST3 a b c" | nc localhost 6381
echo "sadd MYSET3 x y z" | nc localhost 6381
echo "hset MYHASH3 f1 v1" | nc localhost 6381

# Verify on slave
echo "Verifying case-insensitive replication on slave..."
echo "LLEN MYLIST3" | nc localhost 6382
echo "SCARD MYSET3" | nc localhost 6382
echo "HLEN MYHASH3" | nc localhost 6382

# Clean up
echo "Cleaning up..."
kill $MASTER_PID 2>/dev/null || true
kill $SLAVE_PID 2>/dev/null || true
wait $MASTER_PID 2>/dev/null || true
wait $SLAVE_PID 2>/dev/null || true

echo "Data structures replication test completed!"
