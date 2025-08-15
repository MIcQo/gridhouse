#!/bin/bash

echo "Testing Redis Clone MSET/MGET Commands..."

# Start server
echo "Starting server..."
./gridhouse --port :6380 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test MSET command
echo "Testing MSET command..."

# Test MSET with single key-value pair
echo "MSET key1 value1" | nc localhost 6380

# Test MSET with multiple key-value pairs
echo "MSET key2 value2 key3 value3 key4 value4" | nc localhost 6380

# Test MSET with special characters
echo "MSET key:with:colons value:with:colons key-with-dashes value-with-dashes" | nc localhost 6380

# Test MSET with empty values
echo "MSET empty1 \"\" empty2 \"\"" | nc localhost 6380

# Test MGET command
echo "Testing MGET command..."

# Test MGET with single key
echo "MGET key1" | nc localhost 6380

# Test MGET with multiple keys
echo "MGET key1 key2 key3 key4" | nc localhost 6380

# Test MGET with mixed existing and non-existing keys
echo "MGET key1 nonexistent key2 another_nonexistent" | nc localhost 6380

# Test MGET with all non-existing keys
echo "MGET nonexistent1 nonexistent2 nonexistent3" | nc localhost 6380

# Test MGET with special characters
echo "MGET key:with:colons key-with-dashes" | nc localhost 6380

# Test MGET with empty values
echo "MGET empty1 empty2" | nc localhost 6380

# Test MSET/MGET integration
echo "Testing MSET/MGET integration..."

# Set multiple keys with MSET
echo "MSET user:1 alice user:2 bob user:3 charlie" | nc localhost 6380

# Get them back with MGET
echo "MGET user:1 user:2 user:3" | nc localhost 6380

# Test MSET/MGET with data structures
echo "Testing MSET/MGET with data structures..."

# Create some data structures
echo "LPUSH mylist a b c" | nc localhost 6380
echo "SADD myset x y z" | nc localhost 6380
echo "HSET myhash field1 value1" | nc localhost 6380

# Set string keys with MSET
echo "MSET string1 value1 string2 value2" | nc localhost 6380

# Test MGET with mixed data types
echo "MGET string1 mylist string2 myset myhash nonexistent" | nc localhost 6380

# Test performance with many keys
echo "Testing performance with many keys..."

# Set many keys with MSET
keys_values=""
for i in {1..10}; do
    keys_values="$keys_values key$i value$i"
done
echo "MSET $keys_values" | nc localhost 6380

# Get them back with MGET
keys=""
for i in {1..10}; do
    keys="$keys key$i"
done
echo "MGET $keys" | nc localhost 6380

echo "MSET/MGET command test completed!"

# Clean up
echo "Cleaning up..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
