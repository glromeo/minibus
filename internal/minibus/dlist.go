package minibus

import "sync"

type DList[T any] struct {
	mu   sync.Mutex
	head *node[T]
	tail *node[T]
}

// append a detached node To the tail.
func (l *DList[T]) append(n *node[T]) {
	if n == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	// assume n is detached
	n.list = l

	if l.tail == nil {
		l.head, l.tail = n, n
		return
	}
	l.tail.next = n
	n.prev = l.tail
	l.tail = n
}

// push creates a new node and appends it To the tail.
func (l *DList[T]) push(v T) *node[T] {
	n := &node[T]{value: v}
	l.append(n)
	return n
}

// pop removes and returns the head node (detached). Returns nil if empty.
func (l *DList[T]) pop() *node[T] {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := l.head
	if n == nil {
		return nil
	}
	if n.next != nil {
		n.next.prev = nil
	} else {
		l.tail = nil
	}
	l.head = n.next
	n.prev, n.next, n.list = nil, nil, nil
	return n
}

// remove removes n from the list if present (idempotent).
func (l *DList[T]) remove(n *node[T]) {
	if n == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if n.list != l {
		return // already detached or belongs elsewhere
	}
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		l.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		l.tail = n.prev
	}
	n.prev, n.next, n.list = nil, nil, nil
}

// DList node
type node[T any] struct {
	value      T
	prev, next *node[T]
	list       *DList[T] // nil => detached
}

// remove the node itself from the list
func (n *node[T]) remove() {
	if n == nil || n.list == nil {
		return
	}
	n.list.remove(n)
}
