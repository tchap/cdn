package routingtree

import (
	"fmt"
	"math/bits"
)

// IPNet represents an IP subnet.
//
// IPNet.Left and IPNet.Right represent the given 128-bit IP address.
// The address is aligned to the left, i.e. an address less than 128 bits long
// will case IPNet.Right to contain unoccupied trailing zeros.
type IPNet struct {
	Left  uint64
	Right uint64
	// Scope is effectively the number of bits set.
	Scope int
}

// NewIPNet builds an IPNet from the given IP.
func NewIPNet(ip []byte, scope int) IPNet {
	return IPNet{
		Left:  UInt64FromBytes(ip[:8]),
		Right: UInt64FromBytes(ip[8:]),
		Scope: scope,
	}
}

func (s IPNet) CommonPrefixLen(other uint64, max int) int {
	if max > 64 {
		max = 64
	}
	if max > s.Scope {
		max = s.Scope
	}
	return CommonPrefixLen(s.Left, other, max)
}

func (s IPNet) FirstBit() uint64 {
	return FirstBit(s.Left)
}

// Shift shifts the suffix left. Max n is 64.
func (s *IPNet) Shift(n int) {
	// We never shift by more than 64.
	if n > 64 {
		n = 64
	}
	s.Left = s.Left<<n | s.Right>>(64-n)
	s.Right <<= n
	s.Scope -= n
}

func UInt64FromBytes(p []byte) uint64 {
	if len(p) != 8 {
		panic(fmt.Errorf("unexpected byte slice length: %d", len(p)))
	}

	var out uint64
	for i, b := range p {
		d := uint64(b)
		d <<= (7 - i) * 8
		out |= d
	}
	return out
}

func FirstBit(i uint64) uint64 {
	return i >> 63
}

func CommonPrefixLen(prefix, other uint64, max int) int {
	n := bits.LeadingZeros64(prefix ^ other)
	if n > max {
		n = max
	}
	return n
}

// Pop shifts i by n to the left and returns the bits popped.
// The value returned is formatted in the usual way, i.e.
// the bits are aligned to the left.
//
// (popped, shifted) is returned.
func Pop(i uint64, n int) (uint64, uint64) {
	if n > 64 {
		n = 64
	}
	d := 64 - n
	return (i >> d) << d, i << n
}
