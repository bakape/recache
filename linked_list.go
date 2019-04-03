package recache

// Doubly linked list with optimized back and front access
type linkedList struct {
	front, back *node
}

type node struct {
	next, previous *node
	rec            *record
}

// Prepend record to list. Returns pointer to created node.
func (ll *linkedList) Prepend(rec *record) (n *node) {
	n = &node{
		rec: rec,
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

// Return last element data. If list is empty rec=nil.
func (ll *linkedList) Last() (rec *record) {
	if ll.back == nil {
		return
	}
	return ll.back.rec
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
