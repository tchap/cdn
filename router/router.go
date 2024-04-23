package router

import (
	"net"

	"github.com/tchap/cdn/router/internal/routingtree"
)

// Router will panic when bullshit net.IPNet is passed into any method.
type Router struct {
	tree *routingtree.Tree
}

func New() *Router {
	return &Router{tree: routingtree.New()}
}

func (r *Router) InsertRoute(subnet *net.IPNet, pop uint16) {
	scope, _ := subnet.Mask.Size()
	r.tree.InsertRoute([]byte(subnet.IP), scope, pop)
}

func (r *Router) Route(ecs *net.IPNet) (pop uint16, scope int, ok bool) {
	ecsScope, _ := ecs.Mask.Size()
	return r.tree.FindRoute([]byte(ecs.IP), ecsScope)
}
