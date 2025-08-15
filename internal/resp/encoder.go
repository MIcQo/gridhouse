package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"unsafe"
)

type Type int

const (
	SimpleString Type = iota
	Error
	Integer
	BulkString
	Array
)

type Value struct {
	Type   Type
	Str    string
	Int    int64
	Array  []Value
	IsNull bool
}

// Pre-computed byte slices for common responses
var (
	OkResponse         = []byte("+OK\r\n")
	nullResponse       = []byte("$-1\r\n")
	emptyArrayResponse = []byte("*0\r\n")
	crlfBytes          = []byte("\r\n")
	plusByte           = []byte("+")
	minusByte          = []byte("-")
	colonByte          = []byte(":")
	dollarByte         = []byte("$")
	asteriskByte       = []byte("*")
)

// UltraEncode is fastest RESP encoder with minimal allocations
func UltraEncode(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleString:
		return ultraEncodeSimpleString(w, v.Str)
	case Error:
		return ultraEncodeError(w, v.Str)
	case Integer:
		return ultraEncodeInteger(w, v.Int)
	case BulkString:
		return ultraEncodeBulkString(w, v.Str, v.IsNull)
	case Array:
		return ultraEncodeArray(w, v.Array, v.IsNull)
	default:
		return Encode(w, v) // Fallback
	}
}

// Encode is the original RESP encoder (fallback for ultra-optimized version)
func Encode(w io.Writer, v Value) error {
	switch v.Type {
	case SimpleString:
		_, err := fmt.Fprintf(w, "+%s\r\n", v.Str)
		return err
	case Error:
		_, err := fmt.Fprintf(w, "-%s\r\n", v.Str)
		return err
	case Integer:
		_, err := fmt.Fprintf(w, ":%d\r\n", v.Int)
		return err
	case BulkString:
		if v.IsNull {
			_, err := io.WriteString(w, "$-1\r\n")
			return err
		}
		_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v.Str), v.Str)
		return err
	case Array:
		if v.IsNull {
			_, err := io.WriteString(w, "*-1\r\n")
			return err
		}
		bw := bufio.NewWriter(w)
		if _, err := fmt.Fprintf(bw, "*%d\r\n", len(v.Array)); err != nil {
			return err
		}
		for _, el := range v.Array {
			if err := Encode(bw, el); err != nil {
				return err
			}
		}
		return bw.Flush()
	default:
		return fmt.Errorf("unknown type: %v", v.Type)
	}
}

// ultraEncodeSimpleString encodes simple strings with minimal allocs
func ultraEncodeSimpleString(w io.Writer, s string) error {
	// Special case for OK response (most common)
	if s == "OK" {
		_, err := w.Write(OkResponse)
		return err
	}

	// General case
	if _, err := w.Write(plusByte); err != nil {
		return err
	}
	if _, err := w.Write(unsafe.Slice(unsafe.StringData(s), len(s))); err != nil {
		return err
	}
	_, err := w.Write(crlfBytes)
	return err
}

// ultraEncodeError encodes errors with minimal allocs
func ultraEncodeError(w io.Writer, s string) error {
	if _, err := w.Write(minusByte); err != nil {
		return err
	}
	if _, err := w.Write(unsafe.Slice(unsafe.StringData(s), len(s))); err != nil {
		return err
	}
	_, err := w.Write(crlfBytes)
	return err
}

// ultraEncodeInteger encodes integers with optimized conversion
func ultraEncodeInteger(w io.Writer, i int64) error {
	if _, err := w.Write(colonByte); err != nil {
		return err
	}

	// Use pre-allocated buffer for integer conversion
	var ibuf [20]byte
	buf := strconv.AppendInt(ibuf[:0], i, 10)
	if _, err := w.Write(buf); err != nil {
		return err
	}

	_, err := w.Write(crlfBytes)
	return err
}

// ultraEncodeBulkString encodes bulk strings with minimal allocs
func ultraEncodeBulkString(w io.Writer, s string, isNull bool) error {
	if isNull {
		_, err := w.Write(nullResponse)
		return err
	}

	// Write length prefix
	if _, err := w.Write(dollarByte); err != nil {
		return err
	}

	// Use pre-allocated buffer for length conversion
	var lbuf [20]byte
	buf := strconv.AppendInt(lbuf[:0], int64(len(s)), 10)
	if _, err := w.Write(buf); err != nil {
		return err
	}

	if _, err := w.Write(crlfBytes); err != nil {
		return err
	}

	// Write string data
	if _, err := w.Write(unsafe.Slice(unsafe.StringData(s), len(s))); err != nil {
		return err
	}

	_, err := w.Write(crlfBytes)
	return err
}

// ultraEncodeArray encodes arrays with minimal allocs
func ultraEncodeArray(w io.Writer, arr []Value, isNull bool) error {
	if isNull {
		// RESP null array is encoded as *-1\r\n
		if _, err := w.Write(asteriskByte); err != nil {
			return err
		}
		if _, err := w.Write([]byte("-1")); err != nil {
			return err
		}
		_, err := w.Write(crlfBytes)
		return err
	}

	if len(arr) == 0 {
		_, err := w.Write(emptyArrayResponse)
		return err
	}

	// Write array length prefix
	if _, err := w.Write(asteriskByte); err != nil {
		return err
	}

	// Use pre-allocated buffer for length conversion
	var abuf [20]byte
	buf := strconv.AppendInt(abuf[:0], int64(len(arr)), 10)
	if _, err := w.Write(buf); err != nil {
		return err
	}

	if _, err := w.Write(crlfBytes); err != nil {
		return err
	}

	// Write array elements
	for _, el := range arr {
		if err := UltraEncode(w, el); err != nil {
			return err
		}
	}

	return nil
}

// UltraEncodeOK is an ultra-fast encoder for OK responses
func UltraEncodeOK(w io.Writer) error {
	_, err := w.Write(OkResponse)
	return err
}

// UltraEncodeNull is an ultra-fast encoder for null responses
func UltraEncodeNull(w io.Writer) error {
	_, err := w.Write(nullResponse)
	return err
}

// UltraEncodeString is an ultra-fast encoder for string responses
func UltraEncodeString(w io.Writer, s string) error {
	return ultraEncodeBulkString(w, s, false)
}

// UltraEncodeSimpleString is an ultra-fast encoder for simple string responses
func UltraEncodeSimpleString(w io.Writer, s string) error {
	return ultraEncodeSimpleString(w, s)
}

// UltraEncodeInt is an ultra-fast encoder for integer responses
func UltraEncodeInt(w io.Writer, i int64) error {
	return ultraEncodeInteger(w, i)
}

// UltraEncodeError is an ultra-fast encoder for error responses
func UltraEncodeError(w io.Writer, s string) error {
	return ultraEncodeError(w, s)
}
