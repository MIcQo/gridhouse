package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func newR(b []byte) *bufio.Reader { return bufio.NewReader(bytes.NewReader(b)) }

func TestParseSimpleTypes(t *testing.T) {
	payload := append(
		append(EncodeSimpleString("OK"), EncodeError("ERR wrong type")...),
		EncodeInteger(123)...,
	)
	r := newR(payload)
	v, err := Parse(r)
	if err != nil || v.Type != SimpleString || v.Str != "OK" {
		t.Fatalf("simple string parse failed, got %#v err %v", v, err)
	}
	v, err = Parse(r)
	if err != nil || v.Type != Error || v.Str != "ERR wrong type" {
		t.Fatalf("error parse failed, got %#v err %v", v, err)
	}
	v, err = Parse(r)
	if err != nil || v.Type != Integer || v.Int != 123 {
		t.Fatalf("integer parse failed, got %#v err %v", v, err)
	}
}

func TestParseBulkStrings(t *testing.T) {
	r := newR(append(EncodeBulkString([]byte("hello")), EncodeBulkString(nil)...))
	v, err := Parse(r)
	if err != nil || v.Type != BulkString || v.Str != "hello" || v.IsNull {
		t.Fatalf("bulk parse failed, got %#v err %v", v, err)
	}
	v, err = Parse(r)
	if err != nil || v.Type != BulkString || !v.IsNull {
		t.Fatalf("null bulk parse failed, got %#v err %v", v, err)
	}
}

func TestParseArrays(t *testing.T) {
	payload := []byte{}
	payload = append(payload, []byte("*3\r\n")...)
	payload = append(payload, EncodeSimpleString("OK")...)
	payload = append(payload, EncodeInteger(42)...)
	payload = append(payload, EncodeBulkString([]byte("hi"))...)
	r := newR(payload)
	v, err := Parse(r)
	if err != nil || v.Type != Array || len(v.Array) != 3 {
		t.Fatalf("array parse failed, got %#v err %v", v, err)
	}
	if v.Array[0].Type != SimpleString || v.Array[0].Str != "OK" {
		t.Fatal("array element 0 mismatch")
	}
	if v.Array[1].Type != Integer || v.Array[1].Int != 42 {
		t.Fatal("array element 1 mismatch")
	}
	if v.Array[2].Type != BulkString || v.Array[2].Str != "hi" {
		t.Fatal("array element 2 mismatch")
	}
}

func TestNullArray(t *testing.T) {
	r := newR(EncodeNullArray())
	v, err := Parse(r)
	if err != nil || v.Type != Array || !v.IsNull {
		t.Fatalf("null array parse failed, got %#v err %v", v, err)
	}
}

func TestStrictCRLF(t *testing.T) {
	onlyLF := []byte("+OK\n")
	r := newR(onlyLF)
	_, err := Parse(r)
	if !errors.Is(err, ErrBadLineEnding) {
		t.Fatalf("expected ErrBadLineEnding, got %v", err)
	}
}

func TestUltraParseCommand_Single(t *testing.T) {
	req := EncodeArray([]byte("SET"), []byte("k"), []byte("v"))
	r := newR(req)
	cmd, args, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ToUpper(cmd) != "SET" || len(args) != 2 || args[0] != "k" || args[1] != "v" {
		t.Fatalf("command mismatch %q %v", cmd, args)
	}
}

func TestUltraParseCommand_SingleOK(t *testing.T) {
	r := newR([]byte("+OK\r\n"))
	_, _, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUltraParseCommand_Ping(t *testing.T) {
	r := newR([]byte("PING\r\n"))
	_, _, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUltraParsePipeline_TwoCommands(t *testing.T) {
	req := append(
		EncodeArray([]byte("SET"), []byte("k1"), []byte("v1")),
		EncodeArray([]byte("SET"), []byte("k2"), []byte("v2"))...,
	)
	r := newR(req)
	cmds, err := UltraParsePipeline(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 2 {
		t.Fatalf("expected 2 cmds, got %d", len(cmds))
	}
	if strings.Join(cmds[0], ",") != "SET,k1,v1" {
		t.Fatalf("cmd0 mismatch %v", cmds[0])
	}
	if strings.Join(cmds[1], ",") != "SET,k2,v2" {
		t.Fatalf("cmd1 mismatch %v", cmds[1])
	}
}

type shortReader struct {
	b      []byte
	pos, n int
}

func (s *shortReader) Read(p []byte) (int, error) {
	if s.pos >= len(s.b) {
		return 0, io.EOF
	}
	lim := s.pos + s.n
	if lim > len(s.b) {
		lim = len(s.b)
	}
	c := copy(p, s.b[s.pos:lim])
	s.pos += c
	return c, nil
}

func TestShortReads(t *testing.T) {
	req := append(
		EncodeArray([]byte("SET"), []byte("key"), bytes.Repeat([]byte("x"), 4096)),
		EncodeArray([]byte("GET"), []byte("key"))...,
	)
	sr := &shortReader{b: req, n: 7}
	r := bufio.NewReader(sr)
	cmd, args, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "SET" || args[0] != "key" || len(args[1]) != 4096 {
		t.Fatalf("short read SET mismatch")
	}
	cmd, args, err = UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "GET" || args[0] != "key" {
		t.Fatalf("short read GET mismatch")
	}
}

func TestTransactionsPipeline(t *testing.T) {
	var req bytes.Buffer
	req.Write(EncodeArray([]byte("MULTI")))
	req.Write(EncodeArray([]byte("SET"), []byte("k"), []byte("v")))
	req.Write(EncodeArray([]byte("GET"), []byte("k")))
	req.Write(EncodeArray([]byte("EXEC")))
	r := newR(req.Bytes())
	cmds, err := UltraParsePipeline(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{"MULTI"}, {"SET", "k", "v"}, {"GET", "k"}, {"EXEC"}}
	if len(cmds) != len(want) {
		t.Fatalf("expected %d cmds, got %d", len(want), len(cmds))
	}
	for i := range want {
		if strings.Join(cmds[i], ",") != strings.Join(want[i], ",") {
			t.Fatalf("cmd %d mismatch %v vs %v", i, cmds[i], want[i])
		}
	}
}

func TestInvalidFrames(t *testing.T) {
	bad := []byte("*2\r\n$3\r\nSET\r\n$-2\r\n")
	r := newR(bad)
	_, _, err := UltraParseCommand(r, 0)
	if err == nil {
		t.Fatal("expected error")
	}

	bad2 := []byte("$5\r\nhello\n")
	r2 := newR(bad2)
	_, err = Parse(r2)
	if err == nil {
		t.Fatal("expected bad line ending")
	}
}

func TestArrayLimit(t *testing.T) {
	req := []byte("*1001\r\n")
	r := newR(req)
	_, _, err := UltraParseCommand(r, 1000)
	if err == nil {
		t.Fatal("expected size error")
	}
}

func TestNullBulkInCommandBecomesEmptyArg(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString("*2\r\n$3\r\nFOO\r\n$-1\r\n")
	r := newR(buf.Bytes())
	cmd, args, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != "FOO" || len(args) != 1 || args[0] != "" {
		t.Fatalf("null bulk arg handling mismatch %q %v", cmd, args)
	}
}

func TestEncodeHelpersRoundTrip(t *testing.T) {
	var req bytes.Buffer
	req.Write(EncodeArray([]byte("SET"), []byte("a"), []byte("b")))
	req.Write(EncodeArray([]byte("GET"), []byte("a")))
	r := newR(req.Bytes())
	cmd1, args1, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	cmd2, args2, err := UltraParseCommand(r, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cmd1 != "SET" || args1[0] != "a" || args1[1] != "b" {
		t.Fatal("roundtrip 1 mismatch")
	}
	if cmd2 != "GET" || args2[0] != "a" {
		t.Fatal("roundtrip 2 mismatch")
	}
}
