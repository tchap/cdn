# Router

This package implements the subnet router.

## Design

The router uses a radix tree. IP prefixes are stores as `uint64` in each node.
This means that each node contains up to 64 bits of the IP addess
and the minimal tree depth is thus 2.

### Possible Improvements

- Scope is currently stored in each node. This can be computed when walking the tree.
- `internal/routingtree` could be refactored a bit, perhaps to put IPNet and bit operations to a separate package.

## Performance

```
$ go test -benchmem -v -bench . -benchtime=10s -run=^$
goos: darwin
goarch: amd64
pkg: github.com/tchap/cdn/router
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkRouter_Route
BenchmarkRouter_Route-12    	14522048	       830.7 ns/op	       0 B/op	       0 allocs/op
PASS
```

This was run for 10 seconds, which means `Route` was called `1 452 204` times per second. No allocations.
