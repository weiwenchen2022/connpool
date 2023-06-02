// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weiwenchen2022/connpool/dialer"
)

func init() {
	type PoolConn struct {
		p *Pool
		c *poolConn
	}

	var (
		mu        sync.Mutex
		freedFrom = make(map[PoolConn]string)
	)

	getFreedFrom := func(c PoolConn) string {
		mu.Lock()
		defer mu.Unlock()
		return freedFrom[c]
	}
	setFreedFrom := func(c PoolConn, s string) {
		mu.Lock()
		freedFrom[c] = s
		mu.Unlock()
	}
	putConnHook = func(p *Pool, pc *poolConn) {
		idx := -1
		for i, v := range p.freeConn {
			if pc == v {
				idx = i
				break
			}
		}
		if idx >= 0 {
			// print before panic, as panic may get lost due to conflicting panic
			// (all goroutines asleep) elsewhere, since we might not unlock
			// the mutex in freeConn here.
			println("double free of conn. conflicts are:\nA) " + getFreedFrom(PoolConn{p, pc}) + "\n\nand\nB) " + stack())
			panic("double free of conn.")
		}
		setFreedFrom(PoolConn{p, pc}, stack())
	}
}

// pollDuration is an arbitrary interval to wait between checks when polling for
// a condition to occur.
const pollDuration = 5 * time.Millisecond

const (
	fakeNetwork = "foo"
	fakeAddress = "foo"
)

func newTestPool(t testing.TB) *Pool {
	return newTestPoolConnector(t, fdialer.(*fakeDialerCtx).NewConnector(nil))
}

func newTestPoolConnector(t testing.TB, fc *fakeConnector) *Pool {
	p := NewPool(fc)

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	return p
}

func TestPoolPing(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	if err := p.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestNewPool(t *testing.T) {
	p := NewPool(networkConnector{fakeNetwork, fakeAddress, fdialer})
	if p.Dialer() != fdialer {
		t.Fatalf("NewPool should return the dialer of the pool")
	}
}

func TestDialerPanic(t *testing.T) {
	// Test that if dialer panics, pool does not deadlock.
	p, err := New("test", fakeNetwork, fakeAddress)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	expectPanic := func(name string, f func()) {
		defer func() {
			if e := recover(); e == nil {
				t.Fatalf("%s did not panic", name)
			}
		}()
		f()
	}
	expectNotPanic := func(name string, f func()) {
		defer func() {
			if e := recover(); e != nil {
				t.Fatalf("%s panic", name)
			}
		}()
		f()
	}

	expectPanic("Read", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("Read")
		_, _ = conn.Read([]byte("PANIC"))
	})
	// check not deadlocked
	expectNotPanic("Read", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		_, _ = conn.Write([]byte("hello\n"))
		_, _ = conn.Read(make([]byte, 256))
	})

	expectPanic("Write", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("Write")
		_, _ = conn.Write([]byte("hello"))
	})
	// check not deadlocked
	expectNotPanic("Write", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		_, _ = conn.Write([]byte("hello"))
	})

	expectPanic("LocalAddr", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("LocalAddr")
		conn.LocalAddr()
	})
	// check not deadlocked
	expectNotPanic("LocalAddr", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		conn.LocalAddr()
	})

	expectPanic("RemoteAddr", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("RemoteAddr")
		conn.RemoteAddr()
	})
	// check not deadlocked
	expectNotPanic("RemoteAddr", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		conn.RemoteAddr()
	})

	expectPanic("SetDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("SetDeadline")
		conn.SetDeadline(time.Time{})
	})
	// check not deadlocked
	expectNotPanic("SetDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		conn.SetDeadline(time.Time{})
	})

	expectPanic("SetReadDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("SetReadDeadline")
		conn.SetReadDeadline(time.Time{})
	})
	// check not deadlocked
	expectNotPanic("SetReadDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		conn.SetReadDeadline(time.Time{})
	})

	expectPanic("SetWriteDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()

		conn.pc.ci.(*fakeConn).SetPanic("SetWriteDeadline")
		conn.SetWriteDeadline(time.Time{})
	})
	// check not deadlocked
	expectNotPanic("SetWriteDeadline", func() {
		conn, _ := p.Conn(context.Background())
		defer conn.Close()
		conn.SetWriteDeadline(time.Time{})
	})
}

func closePool(t testing.TB, p *Pool) {
	if e := recover(); e != nil {
		fmt.Printf("Panic: %v\n", e)
		panic(e)
	}

	setHookpostCloseConn(func(_ *fakeConn, err error) {
		if err != nil {
			t.Errorf("Error closing fakeConn: %v", err)
		}
	})
	defer setHookpostCloseConn(nil)

	if err := p.Close(); err != nil {
		t.Fatalf("error closing Pool: %v", err)
	}

	var numOpen int
	if !waitCondition(t, func() bool {
		numOpen = p.numOpenConns()
		return numOpen == 0
	}) {
		t.Fatalf("%d connections still open after closing Pool", numOpen)
	}
}

func (p *Pool) numDeps() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.dep)
}

// Dependencies are closed via a goroutine, so this polls waiting for
// numDeps to fall to want, waiting up to nearly the test's deadline.
func (p *Pool) numDepsPoll(t *testing.T, want int) int {
	var n int
	waitCondition(t, func() bool {
		n = p.numDeps()
		return n <= want
	})
	return n
}

func (p *Pool) numFreeConns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.freeConn)
}

func (p *Pool) numOpenConns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.numOpen
}

// clearAllConns closes all connections in p.
func (p *Pool) clearAllConns(t *testing.T) {
	p.SetMaxIdleConns(0)

	if g, w := p.numFreeConns(), 0; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}

	if n := p.numDepsPoll(t, 0); n > 0 {
		t.Errorf("number of dependencies = %d; expected 0", n)
		p.dumpDeps(t)
	}
}

func (p *Pool) dumpDeps(t *testing.T) {
	for fc := range p.dep {
		p.dumpDep(t, 0, fc, make(map[finalCloser]bool))
	}
}

func (p *Pool) dumpDep(t *testing.T, depth int, dep finalCloser, seen map[finalCloser]bool) {
	seen[dep] = true
	indent := strings.Repeat("    ", depth)
	ds := p.dep[dep]
	for k := range ds {
		t.Logf("%s%T (%[2]p) waiting for -> %T (%[3]p)", indent, dep, k)
		if fc, ok := k.(finalCloser); ok {
			if !seen[fc] {
				p.dumpDep(t, depth+1, fc, seen)
			}
		}
	}
}

func waitCondition(t testing.TB, fn func() bool) bool {
	timeout := 5 * time.Second

	type deadliner interface {
		Deadline() (time.Time, bool)
	}

	if td, ok := t.(deadliner); ok {
		if deadline, ok := td.Deadline(); ok {
			timeout = time.Until(deadline)
			timeout = timeout * 19 / 20 // Give 5% headroom for cleanup and error-reporting.
		}
	}

	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return true
		}

		if time.Until(deadline) < pollDuration {
			return false
		}

		time.Sleep(pollDuration)
	}
}

// waitForFree checks db.numFreeConns until either it equals want or
// the maxWait time elapses.
func waitForFree(t *testing.T, p *Pool, want int) {
	var numFree int
	if !waitCondition(t, func() bool {
		numFree = p.numFreeConns()
		return want == numFree
	}) {
		t.Fatalf("free conns after hitting EOF = %d; want %d", numFree, want)
	}
}

func TestPoolExhaustOnCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
	}

	const max = 3
	var saturate, saturateDone sync.WaitGroup
	saturate.Add(max)
	saturateDone.Add(max)

	donePing := make(chan struct{})
	state := 0

	// waiter will be called for all queries, including
	// initial setup queries. The state is only assigned when
	// no queries are made.
	//
	// Only allow the first batch of queries to finish once the
	// second batch of Ping queries have finished.
	waiter := func(ctx context.Context) {
		switch state {
		case 0:
			// Nothing. Initial pool setup.
		case 1:
			saturate.Done()

			select {
			case <-ctx.Done():
			case <-donePing:
			}
		case 2:
		}
	}

	p := newTestPoolConnector(t, fdialer.(*fakeDialerCtx).NewConnector(waiter))
	defer closePool(t, p)

	p.SetMaxOpenConns(max)

	// First saturate the connection pool.
	// Then start new requests for a connection that is canceled after it is requested.
	state = 1
	for i := 0; i < max; i++ {
		go func() {
			err := p.Ping()
			if err != nil {
				t.Errorf("Ping: %v", err)
				return
			}

			saturateDone.Done()
		}()
	}

	saturate.Wait()
	if t.Failed() {
		t.FailNow()
	}

	// Now cancel the request while it is waiting.
	state = 2
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < max; i++ {
		ctxReq, cancelReq := context.WithCancel(ctx)
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancelReq()
		}()

		err := p.PingContext(ctxReq)
		if context.Canceled != err {
			t.Fatalf("PingContext (Exhaust): %v", err)
		}
	}

	close(donePing)
	saturateDone.Wait()

	// Now try to open a normal connection.
	if err := p.PingContext(ctx); err != nil {
		t.Fatalf("PingContext (Normal): %v", err)
	}
}

func TestConnWrite(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := p.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("Hello world")); err != nil {
		t.Fatal(err)
	}

	var b [256]byte
	if _, err := conn.Read(b[:]); err != nil {
		t.Fatal(err)
	}

	if err = conn.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnRaw(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := p.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	sawFunc := false
	err = conn.Raw(func(nc any) error {
		sawFunc = true
		if _, ok := nc.(*fakeConn); !ok {
			return fmt.Errorf("got %T want *fakeConn", nc)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawFunc {
		t.Fatal("Raw func not called")
	}

	func() {
		defer func() {
			if x := recover(); x == nil {
				t.Fatal("expected panic")
			}

			conn.closemu.Lock()
			closed := conn.pc == nil
			conn.closemu.Unlock()
			if !closed {
				t.Fatal("expected connection to be closed after panic")
			}
		}()

		_ = conn.Raw(func(nc any) error {
			panic("Conn.Raw panic should return an error")
		})
		t.Fatal("expected panic from Raw func")
	}()
}

// TestConnIsValid verifies that a database connection that should be discarded,
// is actually discarded and does not re-enter the connection pool.
// If the IsValid method from *fakeConn is removed, this test will fail.
func TestConnIsValid(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(1)

	ctx := context.Background()

	c, err := p.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = c.Raw(func(netConn any) error {
		nc := netConn.(*fakeConn)
		nc.stickyBad = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Close()

	if len(p.freeConn) > 0 && p.freeConn[0].ci.(*fakeConn).stickyBad {
		t.Fatal("bad connection returned to pool; expected bad connection to be discarded")
	}
}

func TestMaxIdleConns(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := len(p.freeConn); got != 1 {
		t.Errorf("freeConns = %d; want 1", got)
	}

	p.SetMaxIdleConns(0)

	if got := len(p.freeConn); got != 0 {
		t.Errorf("freeConns after set to zero = %d; want 0", got)
	}

	conn, err = p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := len(p.freeConn); got != 0 {
		t.Errorf("freeConns = %d; want 0", got)
	}
}

func TestMaxOpenConns(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	setHookpostCloseConn(func(_ *fakeConn, err error) {
		if err != nil {
			t.Errorf("Error closing fakeConn: %v", err)
		}
	})
	defer setHookpostCloseConn(nil)

	p := newTestPool(t)
	defer closePool(t, p)

	dialer := p.Dialer().(*fakeDialerCtx)

	// Force the number of open connections to 0 so we can get an accurate
	// count for the test
	p.clearAllConns(t)

	dialer.mu.Lock()
	opens0 := dialer.openCount
	closes0 := dialer.closeCount
	dialer.mu.Unlock()

	p.SetMaxIdleConns(10)
	p.SetMaxOpenConns(10)

	// Start 50 parallel slow queries.
	const (
		nquery     = 50
		sleepMills = 25 * time.Millisecond
		nbatch     = 2
	)
	msg := fmt.Sprintf("sleep %v\n", sleepMills)

	var wg sync.WaitGroup
	for batch := 0; batch < nbatch; batch++ {
		for i := 0; i < nquery; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, err := p.Conn(context.Background())
				if err != nil {
					t.Error(err)
				}
				defer conn.Close()

				if _, err := conn.Write([]byte(msg)); err != nil {
					t.Error(err)
				}

				var b [256]byte
				if _, err := conn.Read(b[:]); err != nil {
					t.Error(err)
				}
			}()
		}

		// Wait for the batch of queries above to finish before starting the next round.
		wg.Wait()
	}

	if g, w := p.numFreeConns(), 10; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}
	if n := p.numDepsPoll(t, 20); n > 20 {
		t.Errorf("number of dependencies = %d; expected <= 20", n)
		p.dumpDeps(t)
	}

	dialer.mu.Lock()
	opens := dialer.openCount - opens0
	closes := dialer.closeCount - closes0
	dialer.mu.Unlock()

	if opens > 10 {
		t.Logf("open calls = %d", opens)
		t.Logf("close calls = %d", closes)
		t.Errorf("p connections opened = %d; want <= 10", opens)
		p.dumpDeps(t)
	}
	if n := p.numDepsPoll(t, 10); n > 10 {
		t.Errorf("number of dependencies = %d; expected <= 10", n)
		p.dumpDeps(t)
	}

	p.SetMaxOpenConns(5)

	if g, w := p.numFreeConns(), 5; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}
	if n := p.numDepsPoll(t, 5); n > 5 {
		t.Errorf("number of dependencies = %d; expected 0", n)
		p.dumpDeps(t)
	}

	p.SetMaxOpenConns(0)

	if g, w := p.numFreeConns(), 5; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}
	if n := p.numDepsPoll(t, 5); n > 5 {
		t.Errorf("number of dependencies = %d; expected 0", n)
		p.dumpDeps(t)
	}

	p.clearAllConns(t)
}

func TestSingleOpenConn(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(1)

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	// shouldn't deadlock
	conn, err = p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(" world")); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStats(t *testing.T) {
	p := newTestPool(t)
	stats := p.Stats()
	if got := stats.OpenConnections; got != 1 {
		t.Errorf("stats.OpenConnections = %d; want 1", got)
	}

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()

	closePool(t, p)
	stats = p.Stats()
	if got := stats.OpenConnections; got != 0 {
		t.Errorf("stats.OpenConnections = %d; want 0", got)
	}
}

func TestConnMaxLifetime(t *testing.T) {
	t0 := time.Unix(1000000, 0)
	offset := time.Duration(0)

	nowFunc = func() time.Time { return t0.Add(offset) }
	defer func() { nowFunc = time.Now }()

	p := newTestPool(t)
	defer closePool(t, p)

	dialer := p.Dialer().(*fakeDialerCtx)

	// Force the number of open connections to 0 so we can get an accurate
	// count for the test
	p.clearAllConns(t)

	dialer.mu.Lock()
	opens0 := dialer.openCount
	closes0 := dialer.closeCount
	dialer.mu.Unlock()

	p.SetMaxIdleConns(10)
	p.SetMaxOpenConns(10)

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	offset = time.Second
	conn2, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	conn.Close()
	conn2.Close()

	dialer.mu.Lock()
	opens := dialer.openCount - opens0
	closes := dialer.closeCount - closes0
	dialer.mu.Unlock()

	if opens != 2 {
		t.Errorf("opens = %d; want 2", opens)
	}
	if closes != 0 {
		t.Errorf("closes = %d; want 0", closes)
	}
	if g, w := p.numFreeConns(), 2; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}

	// Expire first conn
	offset = 11 * time.Second
	p.SetConnMaxLifetime(10 * time.Second)

	conn, err = p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn2, err = p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	conn2.Close()

	// Give connectionCleaner chance to run.
	waitCondition(t, func() bool {
		dialer.mu.Lock()
		opens = dialer.openCount - opens0
		closes = dialer.closeCount - closes0
		dialer.mu.Unlock()

		return closes == 1
	})

	if opens != 3 {
		t.Errorf("opens = %d; want 3", opens)
	}
	if closes != 1 {
		t.Errorf("closes = %d; want 1", closes)
	}

	if s := p.Stats(); s.MaxLifetimeClosed != 1 {
		t.Errorf("MaxLifetimeClosed = %d; want 1 %#v", s.MaxLifetimeClosed, s)
	}
}

type concurrentTest interface {
	init(t testing.TB, p *Pool)
	finish(t testing.TB)
	test(t testing.TB) error
}

type concurrentPoolTest struct {
	p *Pool
}

func (c *concurrentPoolTest) init(t testing.TB, p *Pool) {
	c.p = p
}

func (c *concurrentPoolTest) finish(t testing.TB) {
	c.p = nil
}

func (c *concurrentPoolTest) test(t testing.TB) error {
	conn, err := c.p.Conn(context.Background())
	if err != nil {
		t.Error(err)
		return err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Error(err)
		return err
	}

	var b [256]byte
	if _, err := conn.Read(b[:]); err != nil {
		t.Error(err)
	}

	return nil
}

func doConcurrentTest(t testing.TB, ct concurrentTest) {
	maxProcs, numReqs := 1, 500
	if testing.Short() {
		maxProcs, numReqs = 4, 50
	}

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(maxProcs))

	p := newTestPool(t)
	defer closePool(t, p)

	ct.init(t, p)
	defer ct.finish(t)

	var wg sync.WaitGroup
	wg.Add(numReqs)

	reqs := make(chan struct{})
	defer close(reqs)

	for i := 0; i < maxProcs*2; i++ {
		go func() {
			for range reqs {
				if err := ct.test(t); err != nil {
					wg.Done()
					continue
				}

				wg.Done()
			}
		}()
	}

	for i := 0; i < numReqs; i++ {
		reqs <- struct{}{}
	}

	wg.Wait()
}

func TestConcurrency(t *testing.T) {
	testCases := []struct {
		name string
		ct   concurrentTest
	}{
		{"Pool", new(concurrentPoolTest)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			doConcurrentTest(t, tc.ct)
		})
	}
}

func TestConnectionLeak(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	// Start by opening defaultMaxIdleConns
	conns := make([]*Conn, defaultMaxIdleConns)

	// We need to SetMaxOpenConns > MaxIdleConns, so the DB can open
	// a new connection and we can fill the idle queue with the released
	// connections.
	p.SetMaxOpenConns(len(conns) + 1)
	for i := range conns {
		c, err := p.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conns[i] = c
	}

	// Now we have defaultMaxIdleConns busy connections. Open
	// a new one, but wait until the busy connections are released
	// before returning control to DB.
	d := p.Dialer().(*fakeDialerCtx)
	d.waitCh = make(chan struct{}, 1)
	d.waitingCh = make(chan struct{}, 1)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		c, err := p.Conn(context.Background())
		if err != nil {
			t.Error(err)
			return
		}

		c.Close()
		wg.Done()
	}()

	// Wait until the goroutine we've just created has started waiting.
	<-d.waitingCh

	// Now close the busy connections. This provides a connection for
	// the blocked goroutine and then fills up the idle queue.
	for _, c := range conns {
		c.Close()
	}

	// At this point we give the new connection to Pool. This connection is
	// now useless, since the idle queue is full and there are no pending
	// requests. Pool should deal with this situation without leaking the
	// connection.
	d.waitCh <- struct{}{}
	wg.Wait()
}

func TestStatsMaxIdleClosedZero(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(1)
	p.SetMaxIdleConns(1)
	p.SetConnMaxLifetime(0)

	preMaxIdleClosed := p.Stats().MaxIdleClosed

	for i := 0; i < 10; i++ {
		conn, err := p.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}

	maxIdleClosed := p.Stats().MaxIdleClosed - preMaxIdleClosed
	t.Logf("MaxIdleClosed: %d", maxIdleClosed)
	if maxIdleClosed != 0 {
		t.Fatal("expected 0 max idle closed conns, got: ", maxIdleClosed)
	}
}

func TestStatsMaxIdleClosedTen(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(1)
	p.SetMaxIdleConns(0)
	p.SetConnMaxLifetime(0)

	preMaxIdleClosed := p.Stats().MaxIdleClosed

	for i := 0; i < 10; i++ {
		conn, err := p.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}

	maxIdleClosed := p.Stats().MaxIdleClosed - preMaxIdleClosed
	t.Logf("MaxIdleClosed: %d", maxIdleClosed)
	if maxIdleClosed != 10 {
		t.Fatal("expected 0 max idle closed conns, got: ", maxIdleClosed)
	}
}

// testUseConns uses count concurrent connections with 1 nanosecond apart.
// Returns the returnedAt time of the final connection.
func testUseConns(t *testing.T, count int, tm time.Time, p *Pool) time.Time {
	conns := make([]*Conn, count)
	ctx := context.Background()
	for i := range conns {
		tm = tm.Add(1 * time.Nanosecond)
		nowFunc = func() time.Time {
			return tm
		}

		c, err := p.Conn(ctx)
		if err != nil {
			t.Error(err)
		}
		conns[i] = c
	}

	for i := len(conns) - 1; i >= 0; i-- {
		tm = tm.Add(1 * time.Nanosecond)
		nowFunc = func() time.Time {
			return tm
		}
		if err := conns[i].Close(); err != nil {
			t.Error(err)
		}
	}

	return tm
}

func TestMaxIdleTime(t *testing.T) {
	const (
		usedConns   = 5
		reusedConns = 2
	)

	testCases := []struct {
		wantMaxIdleTime   time.Duration
		wantMaxLifetime   time.Duration
		wantNextCheck     time.Duration
		wantIdleClosed    int64
		wantMaxIdleClosed int64
		timeOffset        time.Duration
		secondTimeOffset  time.Duration
	}{
		{
			1 * time.Millisecond,
			0,
			time.Millisecond - time.Nanosecond,
			int64(usedConns - reusedConns),
			int64(usedConns - reusedConns),
			10 * time.Millisecond,
			0,
		},
		{
			// Want to close some connections via max idle time and one by max lifetime.
			1 * time.Millisecond,

			// nowFunc() - MaxLifetime should be 1 * time.Nanosecond in connectionCleanerRunLocked.
			// This guarantees that first opened connection is to be closed.
			// Thus it is timeOffset + secondTimeOffset + 3 (+2 for Close while reusing conns and +1 for Conn).
			10*time.Millisecond + 100*time.Nanosecond + 3*time.Nanosecond,
			1 * time.Nanosecond,

			// Closed all not reused connections and extra one by max lifetime.
			int64(usedConns - reusedConns + 1),
			int64(usedConns - reusedConns),
			10 * time.Millisecond,

			// Add second offset because otherwise connections are expired via max lifetime in Close.
			100 * time.Nanosecond,
		},
		{
			1 * time.Hour,
			0,
			1 * time.Second,
			0,
			0,
			10 * time.Millisecond,
			0,
		},
	}

	baseTime := time.Unix(0, 0)
	defer func() {
		nowFunc = time.Now
	}()

	for _, tc := range testCases {
		nowFunc = func() time.Time {
			return baseTime
		}

		t.Run(fmt.Sprintf("%v", tc.wantMaxIdleTime), func(t *testing.T) {
			p := newTestPool(t)
			defer closePool(t, p)

			p.SetMaxOpenConns(usedConns)
			p.SetMaxIdleConns(usedConns)
			p.SetConnMaxIdleTime(tc.wantMaxIdleTime)
			p.SetConnMaxLifetime(tc.wantMaxLifetime)

			preMaxIdleClosed := p.Stats().MaxIdleTimeClosed

			// Busy usedConns.
			testUseConns(t, usedConns, baseTime, p)

			tm := baseTime.Add(tc.timeOffset)

			// Reuse connections which should never be considered idle
			// and exercises the sorting for issue 39471.
			tm = testUseConns(t, reusedConns, tm, p)

			tm = tm.Add(tc.secondTimeOffset)
			nowFunc = func() time.Time {
				return tm
			}

			p.mu.Lock()
			nc, closing := p.connectionCleanerRunLocked(1 * time.Second)
			if nc != tc.wantNextCheck {
				t.Errorf("got %v; want %v next check duration", nc, tc.wantNextCheck)
			}

			// Validate freeConn order.
			var last time.Time
			for _, c := range p.freeConn {
				if last.After(c.returnedAt) {
					t.Error("freeConn is not ordered by returnedAt")
				}
				last = c.returnedAt
			}
			p.mu.Unlock()

			for _, c := range closing {
				c.Close()
			}

			if g, w := int64(len(closing)), tc.wantIdleClosed; w != g {
				t.Errorf("got: %d; want %d closed conns", g, w)
			}

			maxIdleClosed := p.Stats().MaxIdleTimeClosed - preMaxIdleClosed
			if g, w := maxIdleClosed, tc.wantMaxIdleClosed; w != g {
				t.Errorf("got: %d; want %d max idle closed conns", g, w)
			}
		})
	}
}

type pingDialer struct {
	fails bool
}

type pingConn struct {
	net.Conn
	dialer *pingDialer
}

func (pc pingConn) Close() error {
	return nil
}

var errPing = errors.New("Ping failed")

func (pc pingConn) Ping(ctx context.Context) error {
	if pc.dialer.fails {
		return errPing
	}

	return nil
}

var _ dialer.Pinger = pingConn{}

func (pd *pingDialer) Dial(network, address string) (net.Conn, error) {
	return pingConn{dialer: pd}, nil
}

func TestPing(t *testing.T) {
	dialer := &pingDialer{}
	Register("ping", dialer)

	p, err := New("ping", "ignored", "ignored")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := p.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if err := conn.Ping(); err != nil {
		t.Fatalf("err was %#v, expected nil", err)
	}

	dialer.fails = true
	if err := conn.Ping(); errPing != err {
		t.Errorf("err was %#v, expected pingError", err)
	}
}

func BenchmarkConcurrentPool(b *testing.B) {
	b.ReportAllocs()
	ct := new(concurrentPoolTest)
	for i := 0; i < b.N; i++ {
		doConcurrentTest(b, ct)
	}
}
