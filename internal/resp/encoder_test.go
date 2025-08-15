package resp

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeSimpleString(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, Value{Type: SimpleString, Str: "OK"}))
	require.Equal(t, "+OK\r\n", buf.String())
}

func TestEncodeArray(t *testing.T) {
	var buf bytes.Buffer
	arr := Value{Type: Array, Array: []Value{
		{Type: BulkString, Str: "SET"},
		{Type: BulkString, Str: "k"},
		{Type: BulkString, Str: "v"},
	}}
	require.NoError(t, Encode(&buf, arr))
	require.Equal(t, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", buf.String())
}

func TestEncodeNullBulkString(t *testing.T) {
	var buf bytes.Buffer
	nullStr := Value{Type: BulkString, IsNull: true}
	require.NoError(t, Encode(&buf, nullStr))
	require.Equal(t, "$-1\r\n", buf.String())
}

func TestEncodeEmptyBulkString(t *testing.T) {
	var buf bytes.Buffer
	emptyStr := Value{Type: BulkString, Str: ""}
	require.NoError(t, Encode(&buf, emptyStr))
	require.Equal(t, "$0\r\n\r\n", buf.String())
}

func TestEncodeNullArray(t *testing.T) {
	var buf bytes.Buffer
	nullArr := Value{Type: Array, IsNull: true}
	require.NoError(t, Encode(&buf, nullArr))
	require.Equal(t, "*-1\r\n", buf.String())
}

func TestEncodeEmptyArray(t *testing.T) {
	var buf bytes.Buffer
	emptyArr := Value{Type: Array, Array: []Value{}}
	require.NoError(t, Encode(&buf, emptyArr))
	require.Equal(t, "*0\r\n", buf.String())
}

func TestEncodeIntegerAndError(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, Value{Type: Integer, Int: -42}))
	require.Equal(t, ":-42\r\n", buf.String())
	buf.Reset()
	require.NoError(t, Encode(&buf, Value{Type: Error, Str: "ERR wrong"}))
	require.Equal(t, "-ERR wrong\r\n", buf.String())
}

func TestUltraEncode_SimpleString(t *testing.T) {
	var buf bytes.Buffer
	// OK fast path
	require.NoError(t, UltraEncode(&buf, Value{Type: SimpleString, Str: "OK"}))
	require.Equal(t, "+OK\r\n", buf.String())
	buf.Reset()
	// General simple string
	require.NoError(t, UltraEncode(&buf, Value{Type: SimpleString, Str: "PONG"}))
	require.Equal(t, "+PONG\r\n", buf.String())
}

func TestUltraEncode_Integer(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncode(&buf, Value{Type: Integer, Int: 0}))
	require.Equal(t, ":0\r\n", buf.String())
	buf.Reset()
	require.NoError(t, UltraEncode(&buf, Value{Type: Integer, Int: -1234567890}))
	require.Equal(t, ":-1234567890\r\n", buf.String())
}

func TestUltraEncode_BulkString(t *testing.T) {
	var buf bytes.Buffer
	// normal
	require.NoError(t, UltraEncode(&buf, Value{Type: BulkString, Str: "hi"}))
	require.Equal(t, "$2\r\nhi\r\n", buf.String())
	buf.Reset()
	// empty
	require.NoError(t, UltraEncode(&buf, Value{Type: BulkString, Str: ""}))
	require.Equal(t, "$0\r\n\r\n", buf.String())
	buf.Reset()
	// null
	require.NoError(t, UltraEncode(&buf, Value{Type: BulkString, IsNull: true}))
	require.Equal(t, "$-1\r\n", buf.String())
}

func TestUltraEncode_Array(t *testing.T) {
	var buf bytes.Buffer
	// empty array
	require.NoError(t, UltraEncode(&buf, Value{Type: Array, Array: []Value{}}))
	require.Equal(t, "*0\r\n", buf.String())
	buf.Reset()
	// null array
	require.NoError(t, UltraEncode(&buf, Value{Type: Array, IsNull: true}))
	require.Equal(t, "*-1\r\n", buf.String())
	buf.Reset()
	// mixed nested
	arr := Value{Type: Array, Array: []Value{
		{Type: SimpleString, Str: "OK"},
		{Type: Integer, Int: 42},
		{Type: BulkString, Str: "x"},
		{Type: Array, Array: []Value{{Type: BulkString, Str: "y"}}},
	}}
	require.NoError(t, UltraEncode(&buf, arr))
	require.Equal(t, "*4\r\n+OK\r\n:42\r\n$1\r\nx\r\n*1\r\n$1\r\ny\r\n", buf.String())
}

func TestUltraEncode_Wrappers_OK(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncodeOK(&buf))
	require.Equal(t, "+OK\r\n", buf.String())
}

func TestUltraEncode_Wrappers_Null(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncodeNull(&buf))
	require.Equal(t, "$-1\r\n", buf.String())
}

func TestUltraEncode_Wrappers_String(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncodeString(&buf, "xyz"))
	require.Equal(t, "$3\r\nxyz\r\n", buf.String())
}

func TestUltraEncode_Wrappers_SimpleString(t *testing.T) {
	var buf bytes.Buffer
	// generic simple string
	require.NoError(t, UltraEncodeSimpleString(&buf, "PONG"))
	require.Equal(t, "+PONG\r\n", buf.String())
	buf.Reset()
	// OK fast path through wrapper
	require.NoError(t, UltraEncodeSimpleString(&buf, "OK"))
	require.Equal(t, "+OK\r\n", buf.String())
}

func TestUltraEncode_Wrappers_Int(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncodeInt(&buf, 123))
	require.Equal(t, ":123\r\n", buf.String())
}

func TestUltraEncode_Wrappers_Error(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, UltraEncodeError(&buf, "ERR boom"))
	require.Equal(t, "-ERR boom\r\n", buf.String())
}
