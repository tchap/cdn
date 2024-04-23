package routingtree

type Tree struct {
	childZero *treeNode
	childOne  *treeNode
}

func New() *Tree {
	return &Tree{}
}

func (tree *Tree) InsertRoute(ip []byte, scope int, pop uint16) {
	prefix := NewIPNet(ip, scope)

	if prefix.FirstBit() == 1 {
		// Starts with 1.
		if tree.childOne != nil {
			tree.childOne.Insert(prefix, scope, pop)
		} else {
			tree.childOne = newTreeNode(prefix, scope, pop)
		}
	} else {
		// Starts with 0.
		if tree.childZero != nil {
			tree.childZero.Insert(prefix, scope, pop)
		} else {
			tree.childZero = newTreeNode(prefix, scope, pop)
		}
	}
}

func (tree *Tree) FindRoute(ip []byte, scope int) (uint16, int, bool) {
	prefix := NewIPNet(ip, scope)

	if prefix.FirstBit() == 1 {
		return tree.childOne.Find(prefix)
	}
	return tree.childZero.Find(prefix)
}

type treeNode struct {
	prefix    uint64
	prefixLen int

	// In case this is an internal node, the following fields are zero.
	scope int
	pop   uint16

	childZero *treeNode
	childOne  *treeNode
}

// newTreeNode creates a new node for the given part of the IP address.
// The bitmap passed in is therefore a suffix of the whole IP, although
// within the node this is called a prefix as the node is taken as a subtree root.
func newTreeNode(suffix IPNet, scope int, pop uint16) *treeNode {
	if suffix.Scope <= 64 {
		// We can just create a node and return it.
		return &treeNode{
			prefix:    suffix.Left,
			prefixLen: suffix.Scope,
			scope:     scope,
			pop:       pop,
		}
	}

	// Othewise we need to create two nodes in a chain.
	root := &treeNode{
		prefix:    suffix.Left,
		prefixLen: 64,
	}
	child := &treeNode{
		prefix:    suffix.Right,
		prefixLen: suffix.Scope - 64,
		scope:     scope,
		pop:       pop,
	}
	if FirstBit(suffix.Right) == 1 {
		root.childOne = child
	} else {
		root.childZero = child
	}
	return root
}

// Insert inserts the given route into this subtree.
//
// The IP address passed in must share a common prefix with this node, at least a single bit.
// In case the suffix already exists in the subtree, the associated value is replaced.
func (node *treeNode) Insert(suffix IPNet, scope int, pop uint16) {
	commonLen := suffix.CommonPrefixLen(node.prefix, node.prefixLen)
	if commonLen == 0 {
		panic("radix node: insert: suffix common length is 0")
	}

	// We hit the middle of the node prefix, we have to split.
	// We will shorten this node and create two child nodes.
	if commonLen < node.prefixLen {
		commonPrefix, remainingPrefix := Pop(node.prefix, commonLen)

		// One branch is made of the split node.
		// This one keeps the values stored in the current node.
		remainingNode := *node
		remainingNode.prefix = remainingPrefix
		remainingNode.prefixLen = node.prefixLen - commonLen

		// In case suffix is a prefix of the current node, we split without branching.
		if suffix.Scope == commonLen {
			node.prefix = commonPrefix
			node.prefixLen = commonLen
			node.scope = scope
			node.pop = pop
			if FirstBit(remainingNode.prefix) == 1 {
				node.childOne = &remainingNode
				node.childZero = nil
			} else {
				node.childOne = nil
				node.childZero = &remainingNode
			}
			return
		}

		// The other branch is new.
		// We need to drop commonLen bits from the suffix.
		suffix.Shift(commonLen)
		suffixNode := newTreeNode(suffix, scope, pop)

		// Shorten this node.
		node.prefix = commonPrefix
		node.prefixLen = commonLen
		node.scope = 0
		node.pop = 0

		// Link everything together.
		if FirstBit(remainingNode.prefix) == 1 {
			node.childOne = &remainingNode
			node.childZero = suffixNode
		} else {
			node.childOne = suffixNode
			node.childZero = &remainingNode
		}
		return
	}

	if suffix.Scope == commonLen {
		// Exact match. Replace the current value.
		node.scope = scope
		node.pop = pop
		return
	}

	// suffix.Scope > commonLen. We need to dive deeper or create a new branch.
	// But we need to shift first by commonLen to cut this node's part.
	suffix.Shift(commonLen)
	if suffix.FirstBit() == 1 {
		// Starts with 1.
		if node.childOne != nil {
			node.childOne.Insert(suffix, scope, pop)
		} else {
			node.childOne = newTreeNode(suffix, scope, pop)
		}
	} else {
		// Starts with 0.
		if node.childZero != nil {
			node.childZero.Insert(suffix, scope, pop)
		} else {
			node.childZero = newTreeNode(suffix, scope, pop)
		}
	}
}

func (node *treeNode) Find(suffix IPNet) (uint16, int, bool) {
	// Just return false on nil node to save some checks.
	if node == nil {
		return 0, 0, false
	}

	commonLen := suffix.CommonPrefixLen(node.prefix, node.prefixLen)

	// We hit the middle of the node prefix, no match.
	if commonLen < node.prefixLen {
		return 0, 0, false
	}

	// Here we know that commonLen == node.prefixLen
	if suffix.Scope == commonLen {
		// This is exact match. This can still be an internal node, though.
		if node.scope == 0 {
			return 0, 0, false
		}
		return node.pop, node.scope, true
	}

	// suffix.Scope > commonLen. We need to dive deeper.
	// But we need to shift first by suffix.Scope - node.prefixLen to cut this node's part.
	suffix.Shift(commonLen)
	var (
		pop   uint16
		scope int
		ok    bool
	)
	if suffix.FirstBit() == 1 {
		pop, scope, ok = node.childOne.Find(suffix)
	} else {
		pop, scope, ok = node.childZero.Find(suffix)
	}

	// In case there was no exact match in the child, we return this node's value
	// as that is actually the longest subnet prefix.
	if !ok && node.scope != 0 {
		pop, scope, ok = node.pop, node.scope, true
	}
	return pop, scope, ok
}
