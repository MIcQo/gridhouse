package persistence

import "strconv"

// -----------------------------------------------------------------------------
// Tiny helpers that write straight into []byte at given index
// -----------------------------------------------------------------------------

// writeCRLF copies "\r\n" into b starting at idx, returns new index.
func writeCRLF(b []byte, idx int) int {
	b[idx] = '\r'
	b[idx+1] = '\n'
	return idx + 2
}

// writeUint writes decimal representation of non-negative integer n into b
// starting at idx. Returns index directly after written digits.
// This routine does **not** allocate; works completely in-place.
func writeUint(b []byte, idx int, n int) int {
	// Fast path for the common case n < 10 000 (four digits at most).
	// The code is deliberately unrolled to avoid a loop and to keep the
	// generated assembly branch‑free for the typical hot path.
	if n < 10 {
		b[idx] = byte('0' + n)
		return idx + 1
	}
	if n < 100 {
		b[idx] = byte('0' + n/10)
		b[idx+1] = byte('0' + n%10)
		return idx + 2
	}
	if n < 1000 {
		b[idx] = byte('0' + n/100)
		n %= 100
		b[idx+1] = byte('0' + n/10)
		b[idx+2] = byte('0' + n%10)
		return idx + 3
	}
	if n < 10000 {
		b[idx] = byte('0' + n/1000)
		n %= 1000
		b[idx+1] = byte('0' + n/100)
		n %= 100
		b[idx+2] = byte('0' + n/10)
		b[idx+3] = byte('0' + n%10)
		return idx + 4
	}
	// Fallback for the (very unlikely) case n >= 10 000.
	// strconv.AppendInt is already highly optimised for larger numbers,
	// so we delegate to it here.
	tmp := strconv.AppendInt([]byte{}, int64(n), 10)
	copy(b[idx:], tmp)
	return idx + len(tmp)
}

// -----------------------------------------------------------------------------
// Capacity calculation – exact size, no waste
// -----------------------------------------------------------------------------

// exactCap returns the *exact* number of bytes required to encode the given
// command+args in RESP bulk‑string array form.
func exactCap(cmd string, args []string) int {
	// "*<n>\r\n"
	c := 1 + len(strconv.Itoa(1+len(args))) + 2

	// "$<len>\r\n<cmd>\r\n"
	c += 1 + len(strconv.Itoa(len(cmd))) + 2 + len(cmd) + 2

	// each argument: "$<len>\r\n<arg>\r\n"
	for _, a := range args {
		c += 1 + len(strconv.Itoa(len(a))) + 2 + len(a) + 2
	}
	return c
}

// -----------------------------------------------------------------------------
// The new, fully index‑based encoder
// -----------------------------------------------------------------------------

// EncodeRESPArrayFast writes a RESP bulk‑string array for the supplied command
// and arguments directly into a freshly allocated []byte.  The returned slice
// is exactly the length of the encoded message (its capacity equals its length).
func EncodeRESPArrayFast(cmd string, args []string) []byte {
	// 1️⃣ Allocate a buffer of the exact needed size.
	out := make([]byte, exactCap(cmd, args))

	// 2️⃣ Maintain a running write index.
	i := 0

	// -------------------------------------------------
	//  *<total‑elements>\r\n
	// -------------------------------------------------
	out[i] = '*'
	i++
	i = writeUint(out, i, 1+len(args))
	i = writeCRLF(out, i)

	// -------------------------------------------------
	//  $<len(cmd)>\r\n<cmd>\r\n
	// -------------------------------------------------
	out[i] = '$'
	i++
	i = writeUint(out, i, len(cmd))
	i = writeCRLF(out, i)
	copy(out[i:], cmd)
	i += len(cmd)
	i = writeCRLF(out, i)

	// -------------------------------------------------
	//  $<len(arg)>\r\n<arg>\r\n   for each argument
	// -------------------------------------------------
	for _, a := range args {
		out[i] = '$'
		i++
		i = writeUint(out, i, len(a))
		i = writeCRLF(out, i)
		copy(out[i:], a)
		i += len(a)
		i = writeCRLF(out, i)
	}
	// i now equals len(out); returning out[:i] is just a safety net.
	return out[:i]
}
