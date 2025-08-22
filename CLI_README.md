# GridHouse CLI Command

The GridHouse CLI command provides an interactive command-line interface similar to `redis-cli`, allowing you to connect to and interact with GridHouse servers.

## Features

- **Interactive Mode**: Connect to a GridHouse server and execute commands interactively
- **Single Command Mode**: Execute a single command and exit
- **File Mode**: Execute commands from a file
- **Pipe Mode**: Read commands from stdin and write responses to stdout
- **Command History**: Maintains command history for the session
- **RESP Protocol**: Full Redis RESP protocol support for commands and responses
- **Connection Options**: Support for authentication, database selection, and TLS
- **Raw Output**: Option to display raw RESP responses
- **Error Handling**: Comprehensive error handling and reporting

## Usage

### Basic Usage

```bash
# Interactive mode (default)
./gridhouse cli

# Single command mode
./gridhouse cli --eval "PING"

# Connect to specific host and port
./gridhouse cli --host 127.0.0.1 --port 6380
```

### Command Options

#### Connection Options
- `--host`: GridHouse server host (default: 127.0.0.1)
- `-p, --port`: GridHouse server port (default: 6380)
- `--password`: GridHouse server password
- `--db`: Database number (default: 0)
- `--timeout`: Connection timeout (default: 5s)
- `--tls`: Use TLS connection

#### Input/Output Options
- `--raw`: Use raw formatting for replies
- `--eval`: Send specified command
- `--file`: Execute commands from file
- `--pipe`: Pipe mode - read from stdin and write to stdout

## Examples

### Interactive Mode
```bash
./gridhouse cli
```

Example session:
```
GridHouse CLI v1.0.0
Connected to 127.0.0.1:6380
Type 'help' for commands, 'quit' to exit

gridhouse> PING
PONG
gridhouse> SET mykey myvalue
OK
gridhouse> GET mykey
myvalue
gridhouse> quit
Goodbye!
```

### Single Command Mode
```bash
# Basic commands
./gridhouse cli --eval "PING"
./gridhouse cli --eval "SET key value"
./gridhouse cli --eval "GET key"

# Commands with multiple arguments
./gridhouse cli --eval "MSET key1 value1 key2 value2"
./gridhouse cli --eval "MGET key1 key2"
./gridhouse cli --eval "LPUSH mylist item1 item2 item3"
```

### File Mode
```bash
# Create a commands file
cat > commands.txt << EOF
PING
SET filekey filevalue
GET filekey
INCR counter
DBSIZE
EOF

# Execute commands from file
./gridhouse cli --file commands.txt
```

### Pipe Mode
```bash
# Pipe commands from stdin
echo "PING" | ./gridhouse cli --pipe
echo "SET pipekey pipevalue" | ./gridhouse cli --pipe

# Multiple commands
cat commands.txt | ./gridhouse cli --pipe
```

### Raw Output Mode
```bash
# Get raw RESP responses
./gridhouse cli --eval "PING" --raw
# Output: +PONG

./gridhouse cli --eval "SET key value" --raw
# Output: +OK

./gridhouse cli --eval "GET key" --raw
# Output: $5
# value
```

### Connection Options
```bash
# Connect to remote server
./gridhouse cli --host 192.168.1.100 --port 6380

# Use authentication
./gridhouse cli --password mypassword --eval "PING"

# Select different database
./gridhouse cli --db 1 --eval "SET key value"

# Use TLS connection
./gridhouse cli --tls --eval "PING"

# Custom timeout
./gridhouse cli --timeout 10s --eval "PING"
```

## Supported Commands

The CLI supports all GridHouse Redis commands:

### Basic Commands
- `PING` - Test server connectivity
- `ECHO message` - Echo a message
- `QUIT` - Close the connection

### Key-Value Commands
- `SET key value [EX seconds] [PX milliseconds]` - Set a key-value pair
- `GET key` - Get a value by key
- `DEL key [key ...]` - Delete one or more keys
- `EXISTS key [key ...]` - Check if keys exist
- `EXPIRE key seconds` - Set key expiration
- `TTL key` - Get key time to live
- `KEYS pattern` - Find keys matching pattern
- `DBSIZE` - Get number of keys in database
- `FLUSHDB` - Remove all keys from database

### String Commands
- `INCR key` - Increment a counter
- `DECR key` - Decrement a counter
- `INCRBY key increment` - Increment by amount
- `DECRBY key decrement` - Decrement by amount
- `APPEND key value` - Append to string
- `STRLEN key` - Get string length

### List Commands
- `LPUSH key value [value ...]` - Push values to list
- `RPUSH key value [value ...]` - Push values to end of list
- `LPOP key` - Pop value from list
- `RPOP key` - Pop value from end of list
- `LLEN key` - Get list length
- `LRANGE key start stop` - Get range of elements from list
- `LINDEX key index` - Get element by index
- `LREM key count value` - Remove elements from list

### Set Commands
- `SADD key member [member ...]` - Add members to set
- `SREM key member [member ...]` - Remove members from set
- `SMEMBERS key` - Get all members of set
- `SISMEMBER key member` - Check if member exists in set
- `SCARD key` - Get set cardinality
- `SPOP key` - Remove and return random member
- `SRANDMEMBER key [count]` - Get random member(s)

### Hash Commands
- `HSET key field value` - Set hash field
- `HGET key field` - Get hash field
- `HDEL key field [field ...]` - Delete hash fields
- `HGETALL key` - Get all hash fields
- `HKEYS key` - Get all hash keys
- `HVALS key` - Get all hash values
- `HEXISTS key field` - Check if field exists
- `HLEN key` - Get number of hash fields

### Sorted Set Commands
- `ZADD key score member [score member ...]` - Add member to sorted set
- `ZREM key member [member ...]` - Remove members from sorted set
- `ZRANGE key start stop [WITHSCORES]` - Get range from sorted set
- `ZREVRANGE key start stop [WITHSCORES]` - Get reverse range
- `ZCARD key` - Get sorted set cardinality
- `ZSCORE key member` - Get member score
- `ZRANK key member` - Get member rank
- `ZPOPMIN key [count]` - Remove and return members with lowest scores

### Multi-Key Commands
- `MSET key value [key value ...]` - Set multiple keys
- `MGET key [key ...]` - Get multiple values
- `DEL key [key ...]` - Delete multiple keys

### Database Commands
- `SELECT index` - Select database
- `FLUSHDB` - Remove all keys from current database
- `FLUSHALL` - Remove all keys from all databases

## Interactive Mode Features

### Special Commands
- `help` - Show command help
- `quit` or `exit` - Exit the CLI
- `clear` - Clear the screen

### Command History
The CLI maintains command history for the current session:
- Commands are automatically added to history when executed
- Empty commands and duplicate consecutive commands are not added
- History is limited to 100 commands by default
- Full arrow key navigation support for browsing history

### Tab Completion
Basic tab completion is available for common commands.

### Arrow Key Navigation
The interactive mode supports full arrow key navigation:

**History Navigation:**
- **↑ (Up Arrow)**: Navigate to previous command in history
- **↓ (Down Arrow)**: Navigate to next command in history

**Cursor Movement:**
- **← (Left Arrow)**: Move cursor left
- **→ (Right Arrow)**: Move cursor right
- **Home**: Move cursor to start of line
- **End**: Move cursor to end of line

**Editing:**
- **Backspace**: Delete character before cursor
- **Delete**: Delete character at cursor position
- **Enter**: Execute command
- **Ctrl+C**: Interrupt current input (shows helpful message)

**Example Usage:**
```bash
gridhouse> PING
PONG
gridhouse> SET mykey myvalue
OK
gridhouse> GET mykey
myvalue
# Now press ↑ to recall "GET mykey"
# Press ↑ again to recall "SET mykey myvalue"
# Press ↓ to go forward in history
# Use ← → to edit the recalled command
# Press Home to jump to start of line
# Press End to jump to end of line
# Press Ctrl+C to interrupt (shows helpful message)
```

## File Mode Features

### Comments
Lines starting with `#` are treated as comments and ignored:
```
# This is a comment
PING
SET key value  # Inline comment
GET key
```

### Empty Lines
Empty lines are ignored in file mode.

### Error Handling
If a command fails in file mode, the CLI continues with the next command and reports the error.

## RESP Protocol Support

The CLI fully implements the Redis RESP (Redis Serialization Protocol):

### Response Types
- **Simple Strings**: `+OK`, `+PONG`
- **Errors**: `-ERR unknown command`
- **Integers**: `:42`, `:1000`
- **Bulk Strings**: `$5\r\nhello`, `$-1` (null)
- **Arrays**: `*2\r\n$3\r\nfoo\r\n$3\r\nbar`, `*-1` (null)

### Raw Mode
Use `--raw` to see the actual RESP responses:
```bash
./gridhouse cli --eval "PING" --raw
# Output: +PONG

./gridhouse cli --eval "GET nonexistent" --raw
# Output: $-1
```

## Error Handling

### Connection Errors
- Connection refused
- Timeout
- Authentication failed
- TLS errors

### Command Errors
- Unknown command
- Wrong number of arguments
- Invalid data types
- Key not found

### File Errors
- File not found
- Permission denied
- Invalid file format

## Performance Considerations

### Connection Pooling
The CLI creates a new connection for each session. For high-frequency operations, consider using pipe mode or file mode.

### Batch Operations
Use multi-key commands like `MSET` and `MGET` for better performance when working with multiple keys.

### Pipeline Mode
Pipe mode is efficient for processing large numbers of commands:
```bash
cat large_commands.txt | ./gridhouse cli --pipe
```

## Integration Examples

### Shell Scripts
```bash
#!/bin/bash
# Get the value of a key
VALUE=$(./gridhouse cli --eval "GET mykey")
echo "Value: $VALUE"

# Check if key exists
EXISTS=$(./gridhouse cli --eval "EXISTS mykey")
if [ "$EXISTS" = "1" ]; then
    echo "Key exists"
else
    echo "Key does not exist"
fi
```

### Python Integration
```python
import subprocess

def execute_command(cmd):
    result = subprocess.run(['./gridhouse', 'cli', '--eval', cmd], 
                          capture_output=True, text=True)
    return result.stdout.strip()

# Example usage
value = execute_command("GET mykey")
print(f"Value: {value}")
```

### CI/CD Integration
```bash
#!/bin/bash
set -e

# Start GridHouse server
./gridhouse --port :6380 &
SERVER_PID=$!
sleep 3

# Run tests
./gridhouse cli --eval "PING"
./gridhouse cli --eval "SET testkey testvalue"
./gridhouse cli --eval "GET testkey"

# Cleanup
kill $SERVER_PID
```

## Troubleshooting

### Connection Issues
```bash
# Check if server is running
./gridhouse cli --eval "PING"

# Check specific host and port
./gridhouse cli --host 127.0.0.1 --port 6380 --eval "PING"

# Test with timeout
./gridhouse cli --timeout 10s --eval "PING"
```

### Authentication Issues
```bash
# Test without password
./gridhouse cli --eval "PING"

# Test with password
./gridhouse cli --password mypassword --eval "PING"
```

### Command Issues
```bash
# Test basic command
./gridhouse cli --eval "PING"

# Check command syntax
./gridhouse cli --eval "HELP"
```

### Arrow Key Navigation Issues
- **Arrow keys not working**: Terminal raw mode temporarily disabled to fix output formatting
- **Weird line indentation**: Fixed in latest version
- **Ctrl+C ignored**: Fixed in latest version, now shows helpful message

### Terminal Compatibility
The CLI works best with modern terminals that support:
- ANSI escape sequences
- Raw mode input
- Arrow key sequences

**Note**: Terminal raw mode is temporarily disabled to ensure proper output formatting. Arrow key navigation will be re-enabled in a future release once the output formatting issues are resolved.

### File Mode Issues
```bash
# Check file permissions
ls -la commands.txt

# Test with simple file
echo "PING" > test.txt
./gridhouse cli --file test.txt
```

## Comparison with redis-cli

The GridHouse CLI is designed to be compatible with `redis-cli`:

```bash
# GridHouse CLI
./gridhouse cli --eval "PING"

# Redis CLI (equivalent)
redis-cli -p 6380 ping
```

Key differences:
- GridHouse CLI uses `--eval` instead of command-line arguments
- GridHouse CLI provides more detailed error messages
- GridHouse CLI includes built-in help system
- GridHouse CLI supports file mode with comments

## Development

### Adding New Commands
The CLI automatically supports all GridHouse server commands. No additional configuration is needed.

### Custom Response Formatting
Modify the `formatResponse` function to customize response display.

### Extending Interactive Mode
Add new special commands in the `executeInteractive` function.

### Future Enhancements
- Persistent command history across sessions
- Search functionality in command history
- Customizable history size
- Tab completion for commands and keys
