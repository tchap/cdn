package router_test

import (
	"math/rand"
	"testing"
)

func BenchmarkRouter_Route(b *testing.B) {
	// check := assert.New(b)

	// The suite package does not support benchmarks,
	// we need to put this together manually.
	s := new(RouterSuite)
	s.SetT(&testing.T{})
	s.SetupTest()

	routes := s.MustLoadRoutingData()
	s.InitRouter(routes)

	n := len(routes)
	r := s.Router()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		route := routes[rand.Intn(n)]
		r.Route(&route.ECS)
		/*
			pop, scope, ok := r.Route(&route.ECS)
			if check.True(ok) {
				check.Equal(route.Pop, pop)
				check.Equal(route.Scope, scope)
			}
		*/
	}
}
