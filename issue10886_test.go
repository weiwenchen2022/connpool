package connpool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Issue 10886: tests that all connection attempts return when more than
// DB.maxOpen connections are in flight and the first DB.maxOpen fail.
func TestPendingConnsAfterErr(t *testing.T) {
	const (
		maxOpen = 2
		tryOpen = maxOpen*2 + 2
	)

	// No queries will be run.
	p, err := New("test", fakeNetwork, fakeAddress)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer closePool(t, p)
	defer func() {
		for k, v := range p.lastPut {
			t.Logf("%p: %v", k, v)
		}
	}()

	p.SetMaxOpenConns(maxOpen)
	p.SetMaxIdleConns(0)

	errOffline := errors.New("db offline")

	var opening sync.WaitGroup

	setHookOpenErr(func() error {
		// Wait for all connections to enqueue.
		opening.Wait()
		return errOffline
	})
	defer func() { setHookOpenErr(nil) }()

	errs := make(chan error, tryOpen)

	opening.Add(tryOpen)

	for i := 0; i < tryOpen; i++ {
		go func() {
			opening.Done() // signal one connection is in flight
			_, err := p.Conn(context.Background())
			errs <- err
		}()
	}

	opening.Wait() // wait for all workers to begin running

	const timeout = 5 * time.Second
	to := time.NewTimer(timeout)
	defer to.Stop()

	// check that all connections fail without deadlock
	for i := 0; i < tryOpen; i++ {
		select {
		case <-to.C:
			t.Fatalf("orphaned connection request(s), still waiting after %v", timeout)
		case err := <-errs:
			if got, want := err, errOffline; want != got {
				t.Errorf("unexpected err: got %v, want %v", got, want)
			}
		}
	}

	// Wait a reasonable time for the database to close all connections.
	tick := time.NewTicker(3 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-to.C:
			// Closing the pool will check for numOpen and fail the test.
			return
		case <-tick.C:
			p.mu.Lock()
			if p.numOpen == 0 {
				p.mu.Unlock()
				return
			}

			p.mu.Unlock()
		}
	}
}
