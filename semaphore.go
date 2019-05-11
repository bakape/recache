package recache

import (
	"sync/atomic"
)

// Semaphore that blocks all Wait() calls after Init() until Unblock() is
// called.
// After that all Wait() calls don't block.
type semaphore struct {
	finished uint32
	wait     chan struct{}
}

// Initializes the semaphore. Init() must be called before any call to Wait().
func (s *semaphore) Init() {
	s.wait = make(chan struct{})
}

// Unblock any current callers of Wait() and stop blocking future callers
func (s *semaphore) Unblock() {
	atomic.StoreUint32(&s.finished, 1)
	close(s.wait)
}

// Wait for the semaphore to be unblocked, if blocked
func (s *semaphore) Wait() {
	// Hot path after Unblock() call
	if atomic.LoadUint32(&s.finished) == 1 {
		return
	}

	// Block until Unblock() is called
	<-s.wait
}
