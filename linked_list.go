package recache

// Doubly linked list with optimized back and front access
type linkedList struct {
	front, back *node
}

type node struct {
	next, previous *node

	// Storing the location ofd the node instead of an actual *record to reduce
	// load on the GC and the amount of information needed to be stored on the
	// record itself.
	location recordLocation
}

// Prepend record to list. Returns pointer to created node.
func (ll *linkedList) Prepend(loc recordLocation) (n *node) {
	n = &node{
		location: loc,
	}

	if ll.front == nil {
		// Empty list
		ll.front = n
		ll.back = n
		return
	}

	ll.front.previous = n
	n.next = ll.front
	ll.front = n

	return
}

// Return last element data. If list is empty ok=false.
func (ll *linkedList) Last() (loc recordLocation, ok bool) {
	if ll.back == nil {
		return
	}
	return ll.back.location, true
}

// Move existing node to front of list
func (ll *linkedList) MoveToFront(n *node) {
	// Already in front
	if ll.front == n {
		return
	}

	// Join neighbouring nodes
	if n.previous != nil {
		n.previous.next = n.next
		if n == ll.back {
			ll.back = n.previous
		}
	}
	if n.next != nil {
		n.next.previous = n.previous
	}

	// Insert to front
	n.previous = nil
	if ll.front != nil {
		ll.front.previous = n
	}
	n.next = ll.front
	ll.front = n
}

// Remove node from list
func (ll *linkedList) Remove(n *node) {
	// Are both set back to nil, if this was the only node
	if n == ll.front {
		ll.front = n.next
	}
	if n == ll.back {
		ll.back = n.previous
	}

	// Join neighbouring nodes
	if n.previous != nil {
		n.previous.next = n.next
	}
	if n.next != nil {
		n.next.previous = n.previous
	}
}
