package router_test

import (
	"bufio"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/tchap/cdn/router"
)

type routeRecord struct {
	ECS   net.IPNet
	Scope int
	Pop   uint16
}

type RouterSuite struct {
	suite.Suite
	r *router.Router
}

func (s *RouterSuite) SetupTest() {
	s.r = router.New()
}

func (s *RouterSuite) Router() *router.Router {
	return s.r
}

func (s *RouterSuite) MustLoadRoutingData() []routeRecord {
	seedFile, err := os.Open("testdata/routing-data.txt")
	s.Require().NoError(err)
	defer seedFile.Close()

	var routes []routeRecord
	scanner := bufio.NewScanner(seedFile)
	for scanner.Scan() {
		line := scanner.Text()
		var route routeRecord

		// IP
		slashIndex := strings.Index(line, "/")
		s.Require().NotEqual(-1, slashIndex)
		ip := net.ParseIP(line[:slashIndex])
		route.ECS.IP = ip

		// Scope
		spaceIndex := strings.Index(line, " ")
		s.Require().NotEqual(-1, spaceIndex)
		scope, err := strconv.Atoi(line[slashIndex+1 : spaceIndex])
		s.Require().NoError(err)
		route.ECS.Mask = net.CIDRMask(scope, 128)
		route.Scope = scope

		// Pop
		pop, err := strconv.Atoi(line[spaceIndex+1:])
		s.Require().NoError(err)
		route.Pop = uint16(pop)

		routes = append(routes, route)
	}
	s.Require().NoError(scanner.Err())
	return routes
}

func (s *RouterSuite) InitRouter(routes []routeRecord) {
	for i := range routes {
		s.r.InsertRoute(&routes[i].ECS, routes[i].Pop)
	}
}

func (s *RouterSuite) TestRoute_EmptyRouter() {
	ip := net.IPNet{
		IP:   net.ParseIP("2001:49f0:d0b8::"),
		Mask: net.CIDRMask(48, 128),
	}
	pop, scope, ok := s.r.Route(&ip)
	s.False(ok)
	s.Zero(scope)
	s.Zero(pop)
}

func (s *RouterSuite) TestRoute_SingleRoute_Match_Exact() {
	scope := 48
	route := net.IPNet{
		IP:   net.ParseIP("2001:49f0:d0b8::"),
		Mask: net.CIDRMask(scope, 128),
	}
	pop := uint16(174)
	s.r.InsertRoute(&route, pop)

	ecs := route
	gotPop, gotScope, ok := s.r.Route(&ecs)
	if s.True(ok) {
		s.Equal(pop, gotPop)
		s.Equal(scope, gotScope)
	}
}

func (s *RouterSuite) TestRoute_SingleRoute_Match_Subnet() {
	scope := 48
	route := net.IPNet{
		IP:   net.ParseIP("2001:49f0:d0b8::"),
		Mask: net.CIDRMask(scope, 128),
	}
	pop := uint16(174)
	s.r.InsertRoute(&route, pop)

	ecs := net.IPNet{
		IP:   net.ParseIP("2001:49f0:d0b8:2096::"),
		Mask: net.CIDRMask(scope+16, 128),
	}
	gotPop, gotScope, ok := s.r.Route(&ecs)
	if s.True(ok) {
		s.Equal(pop, gotPop)
		s.Equal(scope, gotScope)
	}
}

func (s *RouterSuite) TestRoute_SingleRoute_Mismatch_CommonPrefix() {
	scope := 48
	route := net.IPNet{
		IP:   net.ParseIP("2001:49f0:d0b8::"),
		Mask: net.CIDRMask(scope, 128),
	}
	pop := uint16(174)
	s.r.InsertRoute(&route, pop)

	ecs := net.IPNet{
		IP:   net.ParseIP("2001:49f0::"),
		Mask: net.CIDRMask(32, 128),
	}
	gotPop, gotScope, ok := s.r.Route(&ecs)
	s.False(ok)
	s.Zero(gotPop)
	s.Zero(gotScope)
}

func TestRouter(t *testing.T) {
	suite.Run(t, new(RouterSuite))
}
