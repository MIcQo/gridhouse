#!/bin/bash

echo "=== Final Replication Test ==="

# Clean up any existing processes
pkill -f gridhouse 2>/dev/null
sleep 2

# Clean up data
rm -rf ./data/master ./data/slave
mkdir -p ./data/master ./data/slave

# Start master
echo "1. Starting master server..."
./gridhouse --log-level debug --port 127.0.0.1:6380 --rdb --dir ./data/master > master_final.log 2>&1 &
MASTER_PID=$!
sleep 3

# Verify master is running
if ! echo -e "*1\r\n\$4\r\nPING\r\n" | nc 127.0.0.1 6380 > /dev/null 2>&1; then
    echo "ERROR: Master failed to start"
    kill $MASTER_PID 2>/dev/null
    exit 1
fi
echo "   Master started successfully"

# Start slave
echo "2. Starting slave server..."
./gridhouse --log-level debug --port 127.0.0.1:6381 --slaveof 127.0.0.1:6380 --rdb --dir ./data/slave > slave_final.log 2>&1 &
SLAVE_PID=$!
sleep 5

# Verify slave is running
if ! echo -e "*1\r\n\$4\r\nPING\r\n" | nc 127.0.0.1 6381 > /dev/null 2>&1; then
    echo "ERROR: Slave failed to start"
    kill $MASTER_PID $SLAVE_PID 2>/dev/null
    exit 1
fi
echo "   Slave started successfully"

# Test initial replication with data added BEFORE slave connects
echo "3. Adding initial data..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey1\r\n\$6\r\nvalue1\r\n" | nc 127.0.0.1 6380 > /dev/null
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey2\r\n\$6\r\nvalue2\r\n" | nc 127.0.0.1 6380 > /dev/null
sleep 2

# Check replication
echo "4. Checking initial replication..."
MASTER_DBSIZE=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6380 | grep -o '[0-9]*')
SLAVE_DBSIZE=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6381 | grep -o '[0-9]*')
echo "   Master DBSIZE: $MASTER_DBSIZE"
echo "   Slave DBSIZE: $SLAVE_DBSIZE"

# Test ongoing replication
echo "5. Testing ongoing replication..."
echo -e "*3\r\n\$3\r\nSET\r\n\$4\r\nkey3\r\n\$6\r\nvalue3\r\n" | nc 127.0.0.1 6380 > /dev/null
echo -e "*2\r\n\$4\r\nINCR\r\n\$7\r\ncounter\r\n" | nc 127.0.0.1 6380 > /dev/null
sleep 3

MASTER_DBSIZE_AFTER=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6380 | grep -o '[0-9]*')
SLAVE_DBSIZE_AFTER=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6381 | grep -o '[0-9]*')
echo "   Master DBSIZE after: $MASTER_DBSIZE_AFTER"
echo "   Slave DBSIZE after: $SLAVE_DBSIZE_AFTER"

# Test FLUSHDB replication
echo "6. Testing FLUSHDB replication..."
echo -e "*1\r\n\$7\r\nFLUSHDB\r\n" | nc 127.0.0.1 6380 > /dev/null
sleep 3

MASTER_DBSIZE_FLUSH=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6380 | grep -o '[0-9]*')
SLAVE_DBSIZE_FLUSH=$(echo -e "*1\r\n\$6\r\nDBSIZE\r\n" | nc 127.0.0.1 6381 | grep -o '[0-9]*')
echo "   Master DBSIZE after FLUSHDB: $MASTER_DBSIZE_FLUSH"
echo "   Slave DBSIZE after FLUSHDB: $SLAVE_DBSIZE_FLUSH"

# Check replication stats
echo "7. Replication statistics..."
FORWARDED=$(grep -c "Successfully forwarded" master_final.log 2>/dev/null || echo "0")
RECEIVED=$(grep -c "Received command from master" slave_final.log 2>/dev/null || echo "0")
echo "   Commands forwarded by master: $FORWARDED"
echo "   Commands received by slave: $RECEIVED"

# Check for errors
echo "8. Checking for errors..."
MASTER_ERRORS=$(grep -c -i "error\|failed" master_final.log 2>/dev/null || echo "0")
SLAVE_ERRORS=$(grep -c -i "error\|failed" slave_final.log 2>/dev/null || echo "0")
echo "   Master errors: $MASTER_ERRORS"
echo "   Slave errors: $SLAVE_ERRORS"

if [ "$SLAVE_ERRORS" -gt 0 ]; then
    echo "   Slave error details:"
    grep -i "error\|failed" slave_final.log | head -3
fi

# Clean up
echo "9. Stopping servers..."
kill $MASTER_PID $SLAVE_PID 2>/dev/null
sleep 2

# Summary
echo "=== SUMMARY ==="
if [ "$MASTER_DBSIZE_FLUSH" = "0" ] && [ "$SLAVE_DBSIZE_FLUSH" = "0" ]; then
    echo "✅ FLUSHDB replication: WORKING"
else
    echo "❌ FLUSHDB replication: FAILED"
fi

if [ "$FORWARDED" -gt 0 ] && [ "$RECEIVED" -gt 0 ]; then
    echo "✅ Command forwarding: WORKING"
else
    echo "❌ Command forwarding: FAILED"
fi

if [ "$SLAVE_ERRORS" = "0" ]; then
    echo "✅ No slave errors"
else
    echo "❌ Slave has errors"
fi

echo "=== Test Complete ==="
