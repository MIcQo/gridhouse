package resp

import (
	"bufio"
	"bytes"
	"testing"
)

func benchReader(b *testing.B, payload []byte, perOp int, fn func(r *bufio.Reader)) {
	b.ReportAllocs()
	b.SetBytes(int64(perOp))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bufio.NewReader(bytes.NewReader(payload))
		fn(r)
	}
}

func BenchmarkParse_SimpleString(b *testing.B) {
	p := EncodeSimpleString("OK")
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != SimpleString {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkParse_Error(b *testing.B) {
	p := EncodeError("ERR some error")
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != Error {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkParse_Integer(b *testing.B) {
	p := EncodeInteger(123456789)
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != Integer {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkParse_BulkSmall(b *testing.B) {
	p := EncodeBulkString([]byte("hello"))
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != BulkString {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkParse_BulkLarge_4KB(b *testing.B) {
	val := bytes.Repeat([]byte("x"), 4<<10)
	p := EncodeBulkString(val)
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != BulkString || len(v.Str) != len(val) {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkParse_Array3(b *testing.B) {
	payload := []byte("*3\r\n")
	payload = append(payload, EncodeSimpleString("OK")...)
	payload = append(payload, EncodeInteger(42)...)
	payload = append(payload, EncodeBulkString([]byte("hi"))...)
	benchReader(b, payload, len(payload), func(r *bufio.Reader) {
		v, err := Parse(r)
		if err != nil || v.Type != Array || len(v.Array) != 3 {
			b.Fatalf("bad parse %v %v", v, err)
		}
	})
}

func BenchmarkUltraParseCommand_SET(b *testing.B) {
	p := EncodeArray([]byte("SET"), []byte("k"), []byte("v"))
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		cmd, args, err := UltraParseCommand(r, 0)
		if err != nil || cmd == "" || len(args) != 2 {
			b.Fatalf("bad command %q %v %v", cmd, args, err)
		}
	})
}

func makePipeline(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		buf.Write(EncodeArray([]byte("SET"), []byte("k"), []byte("v")))
	}
	return buf.Bytes()
}

func BenchmarkUltraParsePipeline_16(b *testing.B) {
	p := makePipeline(16)
	// perOp is one pipeline frame, 16 cmds
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		cmds, err := UltraParsePipeline(r, 0)
		if err != nil || len(cmds) != 16 {
			b.Fatalf("bad pipeline %d %v", len(cmds), err)
		}
	})
}

func BenchmarkUltraParsePipeline_128(b *testing.B) {
	p := makePipeline(128)
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		cmds, err := UltraParsePipeline(r, 0)
		if err != nil || len(cmds) != 128 {
			b.Fatalf("bad pipeline %d %v", len(cmds), err)
		}
	})
}

func BenchmarkUltraParseCommand_ShortReads(b *testing.B) {
	val := bytes.Repeat([]byte("x"), 4096)
	p := EncodeArray([]byte("SET"), []byte("key"), val)
	b.ReportAllocs()
	b.SetBytes(int64(len(p)))
	for i := 0; i < b.N; i++ {
		sr := &shortReader{b: p, n: 7}
		r := bufio.NewReader(sr)
		cmd, args, err := UltraParseCommand(r, 0)
		if err != nil || cmd != "SET" || args[0] != "key" || len(args[1]) != len(val) {
			b.Fatalf("bad short read parse %q %v %v", cmd, args, err)
		}
	}
}

func BenchmarkTransactionsPipeline_4(b *testing.B) {
	var req bytes.Buffer
	req.Write(EncodeArray([]byte("MULTI")))
	req.Write(EncodeArray([]byte("SET"), []byte("k"), []byte("v")))
	req.Write(EncodeArray([]byte("GET"), []byte("k")))
	req.Write(EncodeArray([]byte("EXEC")))
	p := req.Bytes()
	benchReader(b, p, len(p), func(r *bufio.Reader) {
		cmds, err := UltraParsePipeline(r, 0)
		if err != nil || len(cmds) != 4 {
			b.Fatalf("bad tx pipeline %d %v", len(cmds), err)
		}
	})
}
