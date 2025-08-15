package aof

import (
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	path := "test_aof.log"
	writer, err := NewWriter(path, No)
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()

	data := []byte("SET key value\n")
	for i := 0; i < b.N; i++ {
		err := writer.Append(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
