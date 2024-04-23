package routingtree_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tchap/cdn/router/internal/routingtree"
)

type IPNetSuite struct {
	suite.Suite
}

func (s *IPNetSuite) TestNew() {
	ip := []byte{
		0b10000000, 0b1000000, 0b100000, 0b10000, 0b1000, 0b100, 0b10, 0b1,
		0b1, 0b10, 0b100, 0b1000, 0b10000, 0b100000, 0b1000000, 0b10000000,
	}
	p := routingtree.NewIPNet(ip, 128)
	s.Equal(routingtree.IPNet{
		Left:  0b1000000001000000001000000001000000001000000001000000001000000001,
		Right: 0b0000000100000010000001000000100000010000001000000100000010000000,
		Scope: 128,
	}, p)
}

func (s *IPNetSuite) TestCommonPrefixLen() {
	p := routingtree.IPNet{
		Left:  0xffff000000000000,
		Scope: 16,
	}
	s.Equal(16, p.CommonPrefixLen(0xffffffff00000000, 32))
	s.Equal(16, p.CommonPrefixLen(0xffff00ffffff0000, 64))
	s.Equal(8, p.CommonPrefixLen(0xffffffff00000000, 8))
	s.Equal(0, p.CommonPrefixLen(0, 32))
}

func (s *IPNetSuite) TestShift() {
	p := routingtree.IPNet{
		Left:  0b1000000001000000001000000001000000001000000001000000001000000001,
		Right: 0b0000000100000010000001000000100000010000001000000100000010000000,
		Scope: 128,
	}
	p.Shift(20)
	s.Equal(routingtree.IPNet{
		Left:  0b0000000100000000100000000100000000100000000100000001000000100000,
		Right: 0b0100000010000001000000100000010000001000000000000000000000000000,
		Scope: 108,
	}, p)
}

func (s *IPNetSuite) TestBits_UInt64FromBytes() {
	in := []byte{0b10000000, 0b1000000, 0b100000, 0b10000, 0b1000, 0b100, 0b10, 0b1}
	expected := uint64(0b1000000001000000001000000001000000001000000001000000001000000001)
	s.Equal(expected, routingtree.UInt64FromBytes(in))
}

func (s *IPNetSuite) TestBits_FirstBit() {
	s.Run("Zero", func() {
		i := uint64(0x7fffffffffffffff)
		s.Equal(uint64(0), routingtree.FirstBit(i))
	})

	s.Run("One", func() {
		i := uint64(0xffffffffffffffff)
		s.Equal(uint64(1), routingtree.FirstBit(i))
	})
}

func (s *IPNetSuite) TestBits_Pop() {
	in := uint64(0xffffffffffffffff)
	pop, remainder := routingtree.Pop(in, 24)
	s.Equal(uint64(0xffffff0000000000), pop)
	s.Equal(uint64(0xffffffffff000000), remainder)
}

func TestIPSuffix(t *testing.T) {
	suite.Run(t, new(IPNetSuite))
}
