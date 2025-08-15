package resp

import (
	"bufio"
	"bytes"
	"testing"
)

// Ultra-optimized encoder benchmarks
func BenchmarkUltraEncodeSimpleString(b *testing.B) {
	value := Value{Type: SimpleString, Str: "Random string"}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeBulkString(b *testing.B) {
	value := Value{Type: BulkString, Str: "hello"}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeInteger(b *testing.B) {
	value := Value{Type: Integer, Int: 123}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeArray(b *testing.B) {
	value := Value{
		Type: Array,
		Array: []Value{
			{Type: BulkString, Str: "foo"},
			{Type: BulkString, Str: "bar"},
		},
	}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeError(b *testing.B) {
	value := Value{Type: Error, Str: "ERR something went wrong"}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeNullBulkString(b *testing.B) {
	value := Value{Type: BulkString, IsNull: true}
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncode(writer, value)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeOK(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncodeOK(writer)
		writer.Flush()
	}
}

func BenchmarkUltraEncodeNull(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		UltraEncodeNull(writer)
		writer.Flush()
	}
}
