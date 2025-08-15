package resp

import (
	"bufio"
	"bytes"
	"testing"
)

// Ultra-optimized parser benchmarks
func BenchmarkUltraParseSimpleString(b *testing.B) {
	data := []byte("+OK\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUltraParseBulkString(b *testing.B) {
	data := []byte("$5\r\nhello\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUltraParseInteger(b *testing.B) {
	data := []byte(":123\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUltraParseArray(b *testing.B) {
	data := []byte("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUltraParseSetCommand(b *testing.B) {
	data := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}

func BenchmarkUltraParseGetCommand(b *testing.B) {
	data := []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReader(bytes.NewReader(data))
		if _, _, err := UltraParseCommand(reader, 1000); err != nil {
			panic(err)
		}
	}
}
