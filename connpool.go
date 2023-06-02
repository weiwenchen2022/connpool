// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package connpool maintains a pool of idle connections for efficient re-use.
package connpool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weiwenchen2022/connpool/dialer"
)

var dialers = struct {
	sync.RWMutex
	m map[string]dialer.Dialer
}{m: make(map[string]dialer.Dialer)}

// nowFunc returns the current time; it's overridden in tests.
var nowFunc = time.Now

// Register makes a dialer available by the provided name.
// If Register is called twice with the same name or if dialer is nil,
// it panics.
func Register(name string, dialer dialer.Dialer) {
	if dialer == nil {
		panic("pool: Register dialer is nil")
	}

	dialers.Lock()
	defer dialers.Unlock()
	if _, dup := dialers.m[name]; dup {
		panic("pool: Register called twice for dialer " + name)
	}
	dialers.m[name] = dialer
}

// For tests.
func unregisterAllDrivers() {
	dialers.Lock()
	defer dialers.Unlock()
	dialers.m = make(map[string]dialer.Dialer)
}

// Dialers returns a sorted list of the names of the registered dialers.
func Dialers() []string {
	dialers.RLock()
	defer dialers.RUnlock()
	list := make([]string, 0, len(dialers.m))
	for name := range dialers.m {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// Pool is a pool of zero or more underlying connections.
// It's safe for concurrent use by multiple goroutines.
//
// The Pool creates and frees connections automatically; it
// also maintains a free pool of idle connections. The pool size
// can be controlled with SetMaxIdleConns.
type Pool struct {
	// Total time waited for new connections.
	waitDuration atomic.Int64

	connector dialer.Connector

	mu       sync.Mutex  // protects following fields
	freeConn []*poolConn // free connections ordered by returnedAt oldest to newest

	connRequests map[uint64]chan connRequest
	nextRequest  uint64 // Next key to use in connRequests.

	numOpen int // number of opened and pending open connections

	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during p.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh chan struct{}

	closed bool
	dep    map[finalCloser]depSet

	lastPut map[*poolConn]string // stacktrace of last conn's put; debug only

	maxIdleCount int           // zero means defaultMaxIdleConns; negative means 0
	maxOpen      int           // <= 0 means unlimited
	maxLifetime  time.Duration // maximum amount of time a connection may be reused
	maxIdleTime  time.Duration // maximum amount of time a connection may be idle before being closed

	cleanerCh chan struct{}

	waitCount         int64 // Total number of connections waited for.
	maxIdleClosed     int64 // Total number of connections closed due to idle count.
	maxIdleTimeClosed int64 // Total number of connections closed due to idle time.
	maxLifetimeClosed int64 // Total number of connections closed due to max connection lifetime limit.

	stop func() // stop cancels the connection opener.
}

// connReuseStrategy determines how (*Pool).conn returns connections.
type connReuseStrategy uint8

const (
	// alwaysNewConn forces a new connection.
	alwaysNewConn connReuseStrategy = iota

	// cachedOrNewConn returns a cached connection, if available, else waits
	// for one to become available (if MaxOpenConns has been reached) or
	// creates a new connection.
	cachedOrNewConn
)

// poolConn wraps a net.Conn with a mutex, to
// be held during all calls into the Conn. (including any calls onto
// interfaces returned via that Conn)
type poolConn struct {
	p *Pool

	createdAt time.Time

	sync.Mutex  // guards following
	ci          net.Conn
	closed      bool
	finalClosed bool // ci.Close has been called

	// guarded by p.mu
	inUse      bool
	returnedAt time.Time // Time the connection was created or returned.
}

func (pc *poolConn) releaseConn(err error) {
	pc.p.putConn(pc, err)
}

func (pc *poolConn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return pc.createdAt.Add(timeout).Before(nowFunc())
}

// validateConnection checks if the connection is valid and can
// still be used.
func (pc *poolConn) validateConnection() bool {
	pc.Lock()
	defer pc.Unlock()

	if cv, ok := pc.ci.(dialer.Validator); ok {
		return cv.IsValid()
	}
	return true
}

// the pc.p's Mutex is held.
func (pc *poolConn) closePoolLocked() func() error {
	pc.Lock()
	defer pc.Unlock()
	if pc.closed {
		return func() error { return errors.New("pool: duplicate poolConn close") }
	}

	pc.closed = true
	return pc.p.removeDepLocked(pc, pc)
}

func (pc *poolConn) Close() error {
	pc.Lock()
	if pc.closed {
		pc.Unlock()
		return errors.New("pool: duplicate poolConn close")
	}

	pc.closed = true
	pc.Unlock() // not defer; removeDep finalClose calls may need to lock

	pc.p.mu.Lock()
	fn := pc.p.removeDepLocked(pc, pc)
	pc.p.mu.Unlock()
	return fn()
}

func (pc *poolConn) finalClose() error {
	var err error
	withLock(pc, func() {
		pc.finalClosed = true
		err = pc.ci.Close()
		pc.ci = nil
	})

	pc.p.mu.Lock()
	pc.p.numOpen--
	pc.p.maybeOpenNewConnections()
	pc.p.mu.Unlock()

	return err
}

// depSet is a finalCloser's outstanding dependencies
type depSet map[any]bool // set of true bools

// The finalCloser interface is used by (*Pool).addDep and related
// dependency reference counting.
type finalCloser interface {
	// finalClose is called when the reference count of an object
	// goes to zero. (*Pool).mu is not held while calling it.
	finalClose() error
}

// addDep notes that x now depends on dep, and x's finalClose won't be
// called until all of x's dependencies are removed with removeDep.
func (p *Pool) addDep(x finalCloser, dep any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addDepLocked(x, dep)
}

func (p *Pool) addDepLocked(x finalCloser, dep any) {
	if p.dep == nil {
		p.dep = make(map[finalCloser]depSet)
	}

	xdep := p.dep[x]
	if xdep == nil {
		xdep = make(depSet)
		p.dep[x] = xdep
	}
	xdep[dep] = true
}

// removeDep notes that x no longer depends on dep.
// If x still has dependencies, nil is returned.
// If x no longer has any dependencies, its finalClose method will be
// called and its error value will be returned.
func (p *Pool) removeDep(x finalCloser, dep any) error {
	p.mu.Lock()
	fn := p.removeDepLocked(x, dep)
	p.mu.Unlock()
	return fn()
}

func (p *Pool) removeDepLocked(x finalCloser, dep any) func() error {
	xdep, ok := p.dep[x]
	if !ok {
		panic(fmt.Sprintf("unpaired removeDep: no deps for %T", x))
	}

	l0 := len(xdep)
	delete(xdep, dep)

	switch len(xdep) {
	case l0:
		// Nothing removed. Shouldn't happen.
		panic(fmt.Sprintf("unpaired removeDep: no %T dep on %T", dep, x))
	case 0:
		// No more dependencies.
		delete(p.dep, x)
		return x.finalClose
	default:
		// Dependencies remain.
		return func() error { return nil }
	}
}

func (pc *poolConn) ping(ctx context.Context, release func(error)) (err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	if pinger, ok := pc.ci.(dialer.Pinger); ok {
		withLock(pc, func() {
			err = pinger.Ping(ctx)
		})
	}

	return err
}

func (pc *poolConn) read(release func(error), b []byte) (n int, err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	withLock(pc, func() {
		n, err = pc.ci.Read(b)
	})
	return n, err
}

func (pc *poolConn) write(release func(error), b []byte) (n int, err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	withLock(pc, func() {
		n, err = pc.ci.Write(b)
	})
	return n, err
}

func (pc *poolConn) localAddr(release func(error)) net.Addr {
	defer func() {
		var r any
		var err error
		if r = recover(); r != nil {
			err = net.ErrClosed
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	var addr net.Addr
	withLock(pc, func() {
		addr = pc.ci.LocalAddr()
	})
	return addr
}

func (pc *poolConn) remoteAddr(release func(error)) net.Addr {
	defer func() {
		var r any
		var err error
		if r = recover(); r != nil {
			err = net.ErrClosed
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	var addr net.Addr
	withLock(pc, func() {
		addr = pc.ci.RemoteAddr()
	})
	return addr
}

func (pc *poolConn) setDeadline(release func(error), t time.Time) (err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	withLock(pc, func() {
		err = pc.ci.SetDeadline(t)
	})
	return err
}

func (pc *poolConn) setReadDeadline(release func(error), t time.Time) (err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	withLock(pc, func() {
		err = pc.ci.SetReadDeadline(t)
	})
	return err
}

func (pc *poolConn) setWriteDeadline(release func(error), t time.Time) (err error) {
	defer func() {
		var r any
		if r = recover(); r != nil {
			if err == nil {
				err = net.ErrClosed
			}
		}
		release(err)

		if r != nil {
			panic(r)
		}
	}()

	withLock(pc, func() {
		err = pc.ci.SetWriteDeadline(t)
	})
	return err
}

// This is the size of the connectionOpener request chan (Pool.openerCh).
// This value should be larger than the maximum typical value
// used for p.maxOpen. If maxOpen is significantly larger than
// connectionRequestQueueSize then it is possible for ALL calls into the *Pool
// to block until the connectionOpener can satisfy the backlog of requests.
const connectionRequestQueueSize = 1000_000

type networkConnector struct {
	network, address string
	dialer           dialer.Dialer
}

func (t networkConnector) Connect(context.Context) (net.Conn, error) {
	return t.dialer.Dial(t.network, t.address)
}

func (t networkConnector) Dialer() dialer.Dialer {
	return t.dialer
}

// NewPool opens a pool using a Connector, allowing dialers to
// bypass network and address.
//
// NewPool may just validate its arguments without creating a connection.
// To verify that the network and address is valid, call Ping.
//
// The returned Pool is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. It is rarely necessary to
// close a Pool.
func NewPool(c dialer.Connector) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		connector:    c,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		lastPut:      make(map[*poolConn]string),
		connRequests: make(map[uint64]chan connRequest),
		stop:         cancel,
	}

	go p.connectionOpener(ctx)

	return p
}

// New opens a pool specified by its dialer name and a
// dialer-specific network and address.
//
// New may just validate its arguments without creating a connection.
// To verify that the network and address is valid, call Ping.
//
// The returned Pool is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections.
// It is rarely necessary to close a Pool.
func New(dialerName, network, address string) (*Pool, error) {
	dialers.RLock()
	dialeri, ok := dialers.m[dialerName]
	dialers.RUnlock()
	if !ok {
		return nil, fmt.Errorf("pool: unknown dialer %q (forgotten import?)", dialerName)
	}

	if dialerCtx, ok := dialeri.(dialer.DialerContext); ok {
		connector, err := dialerCtx.OpenConnector(network, address)
		if err != nil {
			return nil, err
		}

		return NewPool(connector), nil
	}

	return NewPool(networkConnector{network, address, dialeri}), nil
}

func (p *Pool) pingPC(ctx context.Context, pc *poolConn, release func(error)) (err error) {
	defer func() {
		release(err)
	}()

	if pinger, ok := pc.ci.(dialer.Pinger); ok {
		withLock(pc, func() {
			err = pinger.Ping(ctx)
		})
	}

	return err
}

// PingContext verifies a connection is still alive,
// establishing a connection if necessary.
func (p *Pool) PingContext(ctx context.Context) error {
	var err error

	err = p.retry(func(strategy connReuseStrategy) error {
		err = p.ping(ctx, strategy)
		return err
	})

	return err
}

// Ping verifies a connection is still alive,
// establishing a connection if necessary.
//
// Ping uses context.Background internally; to specify the context, use
// PingContext.
func (p *Pool) Ping() error {
	return p.PingContext(context.Background())
}

func (p *Pool) ping(ctx context.Context, strategy connReuseStrategy) error {
	pc, err := p.conn(ctx, strategy)
	if err != nil {
		return err
	}

	return p.pingPC(ctx, pc, pc.releaseConn)
}

// Close closes the pool.
// Close then waits for all operations that have started
// to finish.
//
// It is rare to Close a Pool, as the Pool is meant to be
// long-lived and shared between many goroutines.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed { // Make Pool.Close idempotent
		p.mu.Unlock()
		return nil
	}

	if p.cleanerCh != nil {
		close(p.cleanerCh)
	}

	var err error
	fns := make([]func() error, 0, len(p.freeConn))
	for _, pc := range p.freeConn {
		fns = append(fns, pc.closePoolLocked())
	}
	p.freeConn = nil
	p.closed = true

	for _, req := range p.connRequests {
		close(req)
	}
	p.mu.Unlock()

	for _, fn := range fns {
		if err1 := fn(); err1 != nil {
			err = err1
		}
	}
	p.stop()

	if c, ok := p.connector.(io.Closer); ok {
		if err1 := c.Close(); err1 != nil {
			err = err1
		}
	}

	return err
}

const defaultMaxIdleConns = 2

func (p *Pool) maxIdleConnsLocked() int {
	n := p.maxIdleCount
	switch {
	case n == 0:
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}

func (p *Pool) shortestIdleTimeLocked() time.Duration {
	if p.maxIdleTime <= 0 {
		return p.maxLifetime
	}

	if p.maxLifetime <= 0 {
		return p.maxIdleTime
	}

	min := p.maxIdleTime
	if p.maxLifetime < min {
		min = p.maxLifetime
	}
	return min
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func (p *Pool) SetMaxIdleConns(n int) {
	p.mu.Lock()
	if n > 0 {
		p.maxIdleCount = n
	} else {
		// No idle connections.
		p.maxIdleCount = -1
	}

	// Make sure maxIdle doesn't exceed maxOpen
	if p.maxOpen > 0 && p.maxIdleConnsLocked() > p.maxOpen {
		p.maxIdleCount = p.maxOpen
	}

	var closing []*poolConn
	idleCount := len(p.freeConn)
	maxIdle := p.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = p.freeConn[maxIdle:]
		p.freeConn = p.freeConn[:maxIdle]
	}

	p.maxIdleClosed += int64(len(closing))
	p.mu.Unlock()

	for _, c := range closing {
		c.Close()
	}
}

// SetMaxOpenConns sets the maximum number of open connections.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (p *Pool) SetMaxOpenConns(n int) {
	p.mu.Lock()
	p.maxOpen = n
	if n < 0 {
		p.maxOpen = 0
	}

	syncMaxIdle := p.maxOpen > 0 && p.maxIdleConnsLocked() > p.maxOpen
	p.mu.Unlock()

	if syncMaxIdle {
		p.SetMaxIdleConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's age.
func (p *Pool) SetConnMaxLifetime(d time.Duration) {
	if d < 0 {
		d = 0
	}

	p.mu.Lock()
	// Wake cleaner up when lifetime is shortened.
	if d > 0 && d < p.maxLifetime && p.cleanerCh != nil {
		select {
		case p.cleanerCh <- struct{}{}:
		default:
		}
	}

	p.maxLifetime = d
	p.startCleanerLocked()
	p.mu.Unlock()
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's idle time.
func (p *Pool) SetConnMaxIdleTime(d time.Duration) {
	if d < 0 {
		d = 0
	}

	p.mu.Lock()
	// Wake cleaner up when idle time is shortened.
	if d > 0 && d < p.maxIdleTime && p.cleanerCh != nil {
		select {
		case p.cleanerCh <- struct{}{}:
		default:
		}
	}
	p.maxIdleTime = d
	p.startCleanerLocked()
	p.mu.Unlock()
}

// startCleanerLocked starts connectionCleaner if needed.
func (p *Pool) startCleanerLocked() {
	if (p.maxLifetime > 0 || p.maxIdleTime > 0) && p.numOpen > 0 && p.cleanerCh == nil {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionCleaner(p.shortestIdleTimeLocked())
	}
}

func (p *Pool) connectionCleaner(d time.Duration) {
	const minInterval = 1 * time.Second

	if d < minInterval {
		d = minInterval
	}

	t := time.NewTimer(d)

	for {
		select {
		case <-p.cleanerCh: // maxLifetime was changed or p was closed.
		case <-t.C:
		}

		p.mu.Lock()
		d = p.shortestIdleTimeLocked()
		if p.closed || p.numOpen == 0 || d <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}

		d, closing := p.connectionCleanerRunLocked(d)
		p.mu.Unlock()

		for _, c := range closing {
			c.Close()
		}

		if d < minInterval {
			d = minInterval
		}

		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}
}

// connectionCleanerRunLocked removes connections that should be closed from
// freeConn and returns them along side an updated duration to the next check
// if a quicker check is required to ensure connections are checked appropriately.
func (p *Pool) connectionCleanerRunLocked(d time.Duration) (time.Duration, []*poolConn) {
	var idleClosing int64
	var closing []*poolConn

	if p.maxIdleTime > 0 {
		// As freeConn is ordered by returnedAt process
		// in reverse order to minimise the work needed.
		idleSince := nowFunc().Add(-p.maxIdleTime)
		last := len(p.freeConn) - 1
		for i := last; i >= 0; i-- {
			c := p.freeConn[i]
			if c.returnedAt.Before(idleSince) {
				i++
				closing = p.freeConn[:i:i]
				p.freeConn = p.freeConn[i:]
				idleClosing = int64(len(closing))
				p.maxIdleTimeClosed += idleClosing
				break
			}
		}

		if len(p.freeConn) > 0 {
			c := p.freeConn[0]
			if d2 := c.returnedAt.Sub(idleSince); d2 < d {
				// Ensure idle connections are cleaned up as soon as
				// possible.
				d = d2
			}
		}
	}

	if p.maxLifetime > 0 {
		expiredSince := nowFunc().Add(-p.maxLifetime)
		for i := 0; i < len(p.freeConn); i++ {
			c := p.freeConn[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)

				last := len(p.freeConn) - 1
				// Use slow delete as order is required to ensure
				// connections are reused least idle time first.
				copy(p.freeConn[i:], p.freeConn[i+1:])
				p.freeConn[last] = nil
				p.freeConn = p.freeConn[:last]
				i--
			} else if d2 := c.createdAt.Sub(expiredSince); d2 < d {
				// Prevent connections sitting the freeConn when they
				// have expired by updating our next deadline d.
				d = d2
			}
		}
		p.maxLifetimeClosed += int64(len(closing)) - idleClosing
	}

	return d, closing
}

// PoolStats contains pool statistics.
type PoolStats struct {
	MaxOpenConnections int // Maximum number of open connections.

	// Pool Status
	OpenConnections int // The number of established connections both in use and idle.
	InUse           int // The number of connections currently in use.
	Idle            int // The number of idle connections.

	// Counters
	WaitCount         int64         // The total number of connections waited for.
	WaitDuration      time.Duration // The total time blocked waiting for a new connection.
	MaxIdleClosed     int64         // The total number of connections closed due to SetMaxIdleConns.
	MaxIdleTimeClosed int64         // The total number of connections closed due to SetConnMaxIdleTime.
	MaxLifetimeClosed int64         // The total number of connections closed due to SetConnMaxLifetime.
}

// Stats returns pool statistics.
func (p *Pool) Stats() PoolStats {
	wait := p.waitDuration.Load()

	p.mu.Lock()
	defer p.mu.Unlock()

	stats := PoolStats{
		MaxOpenConnections: p.maxOpen,

		OpenConnections: p.numOpen,
		InUse:           p.numOpen - len(p.freeConn),
		Idle:            len(p.freeConn),

		WaitCount:         p.waitCount,
		WaitDuration:      time.Duration(wait),
		MaxIdleClosed:     p.maxIdleClosed,
		MaxIdleTimeClosed: p.maxIdleTimeClosed,
		MaxLifetimeClosed: p.maxLifetimeClosed,
	}
	return stats
}

// Assumes p.mu is locked.
// If there are connRequests and the connection limit hasn't been reached,
// then tell the connectionOpener to open new connections.
func (p *Pool) maybeOpenNewConnections() {
	numRequests := len(p.connRequests)
	if p.maxOpen > 0 {
		numCanOpen := p.maxOpen - p.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}

	for numRequests > 0 {
		p.numOpen++ // optimistically
		numRequests--
		if p.closed {
			return
		}

		p.openerCh <- struct{}{}
	}
}

// Runs in a separate goroutine, opens new connections when requested.
func (p *Pool) connectionOpener(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.openerCh:
			p.openNewConnection(ctx)
		}
	}
}

// Open one new connection
func (p *Pool) openNewConnection(ctx context.Context) {
	// maybeOpenNewConnections has already executed p.numOpen++ before it sent
	// on p.openerCh. This function must execute p.numOpen-- if the
	// connection fails or is closed before returning.
	ci, err := p.connector.Connect(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		if err == nil {
			ci.Close()
		}

		p.numOpen--
		return
	}

	if err != nil {
		p.numOpen--
		p.putConnPoolLocked(nil, err)
		p.maybeOpenNewConnections()
		return
	}

	pc := &poolConn{
		p:          p,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
	}
	if p.putConnPoolLocked(pc, err) {
		p.addDepLocked(pc, pc)
	} else {
		p.numOpen--
		ci.Close()
	}
}

// connRequest represents one request for a new connection
// When there are no idle connections available, Pool.conn will create
// a new connRequest and put it on the p.connRequests list.
type connRequest struct {
	conn *poolConn
	err  error
}

var errPoolClosed = errors.New("pool: pool is closed")

// nextRequestKeyLocked returns the next connection request key.
// It is assumed that nextRequest will not overflow.
func (p *Pool) nextRequestKeyLocked() uint64 {
	next := p.nextRequest
	p.nextRequest++
	return next
}

// conn returns a newly-opened or cached *poolConn.
func (p *Pool) conn(ctx context.Context, strategy connReuseStrategy) (*poolConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errPoolClosed
	}

	// Check if the context is expired.
	select {
	case <-ctx.Done():
		p.mu.Unlock()
		return nil, ctx.Err()
	default:
	}

	lifetime := p.maxLifetime

	// Prefer a free connection, if possible.
	last := len(p.freeConn) - 1
	if cachedOrNewConn == strategy && last >= 0 {
		// Reuse the lowest idle time connection so we can close
		// connections which remain idle as soon as possible.
		conn := p.freeConn[last]
		p.freeConn = p.freeConn[:last]
		conn.inUse = true

		if conn.expired(lifetime) {
			p.maxLifetimeClosed++
			p.mu.Unlock()
			conn.Close()
			return nil, net.ErrClosed
		}
		p.mu.Unlock()

		return conn, nil
	}

	// Out of free connections or we were asked not to use one. If we're not
	// allowed to open any more connections, make a request and wait.
	if p.maxOpen > 0 && p.numOpen >= p.maxOpen {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		req := make(chan connRequest, 1)
		reqKey := p.nextRequestKeyLocked()
		p.connRequests[reqKey] = req
		p.waitCount++
		p.mu.Unlock()

		waitStart := nowFunc()

		// Timeout the connection request with the context.
		select {
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			p.mu.Lock()
			delete(p.connRequests, reqKey)
			p.mu.Unlock()

			p.waitDuration.Add(int64(time.Since(waitStart)))

			select {
			case ret, ok := <-req:
				if ok && ret.conn != nil {
					p.putConn(ret.conn, ret.err)
				}
			default:
			}

			return nil, ctx.Err()
		case ret, ok := <-req:
			p.waitDuration.Add(int64(time.Since(waitStart)))

			if !ok {
				return nil, errPoolClosed
			}

			// Only check if the connection is expired if the strategy is cachedOrNewConns.
			// If we require a new connection, just re-use the connection without looking
			// at the expiry time. If it is expired, it will be checked when it is placed
			// back into the connection pool.
			// This prioritizes giving a valid connection to a client over the exact connection
			// lifetime, which could expire exactly after this point anyway.
			if cachedOrNewConn == strategy && ret.err == nil && ret.conn.expired(lifetime) {
				p.mu.Lock()
				p.maxLifetimeClosed++
				p.mu.Unlock()
				ret.conn.Close()
				return nil, net.ErrClosed
			}

			if ret.conn == nil {
				return nil, ret.err
			}

			return ret.conn, ret.err
		}
	}

	p.numOpen++ // optimistically
	p.mu.Unlock()
	ci, err := p.connector.Connect(ctx)
	if err != nil {
		p.mu.Lock()
		p.numOpen-- // correct for earlier optimism
		p.maybeOpenNewConnections()
		p.mu.Unlock()
		return nil, err
	}

	p.mu.Lock()
	pc := &poolConn{
		p:          p,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
		inUse:      true,
	}
	p.addDepLocked(pc, pc)
	p.mu.Unlock()
	return pc, nil
}

// putConnHook is a hook for testing.
var putConnHook func(*Pool, *poolConn)

// debugGetPut determines whether getConn & putConn calls' stack traces
// are returned for more verbose crashes.
const debugGetPut = false

// putConn adds a connection to the p's free pool.
// err is optionally the last error that occurred on this connection.
func (p *Pool) putConn(pc *poolConn, err error) {
	if !errors.Is(err, net.ErrClosed) {
		if !pc.validateConnection() {
			err = net.ErrClosed
		}
	}

	p.mu.Lock()
	if !pc.inUse {
		p.mu.Unlock()
		if debugGetPut {
			fmt.Printf("putConn(%v) DUPLICATE was: %s\n\nPREVIOUS was: %s", pc, stack(), p.lastPut[pc])
		}

		panic("connection returned that was never out")
	}

	if !errors.Is(err, net.ErrClosed) && pc.expired(p.maxLifetime) {
		p.maxLifetimeClosed++
		err = net.ErrClosed
	}

	if debugGetPut {
		p.lastPut[pc] = stack()
	}

	pc.inUse = false
	pc.returnedAt = nowFunc()

	if errors.Is(err, net.ErrClosed) {
		// Don't reuse bad connections.
		// Since the conn is considered bad and is being discarded, treat it
		// as closed. Don't decrement the open count here, finalClose will
		// take care of that.
		p.maybeOpenNewConnections()
		p.mu.Unlock()
		pc.Close()
		return
	}

	if putConnHook != nil {
		putConnHook(p, pc)
	}

	added := p.putConnPoolLocked(pc, nil)
	p.mu.Unlock()

	if !added {
		pc.Close()
		return
	}
}

// Satisfy a connRequest or put the poolConn in the idle pool and return true
// or return false.
// putConnPoolLocked will satisfy a connRequest if there is one, or it will
// return the *poolConn to the freeConn list if err == nil and the idle
// connection limit will not be exceeded.
// If err != nil, the value of pc is ignored.
// If err == nil, then pc must not equal nil.
// If a connRequest was fulfilled or the *poolConn was placed in the
// freeConn list, then true is returned, otherwise false is returned.
func (p *Pool) putConnPoolLocked(pc *poolConn, err error) bool {
	if p.closed {
		return false
	}

	if p.maxOpen > 0 && p.numOpen > p.maxOpen {
		return false
	}

	if c := len(p.connRequests); c > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range p.connRequests {
			break
		}

		delete(p.connRequests, reqKey) // Remove from pending requests.
		if err == nil {
			pc.inUse = true
		}

		req <- connRequest{
			conn: pc,
			err:  err,
		}

		return true
	} else if err == nil && !p.closed {
		if p.maxIdleConnsLocked() > len(p.freeConn) {
			p.freeConn = append(p.freeConn, pc)
			p.startCleanerLocked()
			return true
		}

		p.maxIdleClosed++
	}

	return false
}

// maxClosedConnRetries is the number of maximum retries if the dialer returns
// net.ErrClosed to signal a broken connection before forcing a new
// connection to be opened.
const maxClosedConnRetries = 2

func (p *Pool) retry(fn func(strategy connReuseStrategy) error) error {
	for i := int64(0); i < maxClosedConnRetries; i++ {
		err := fn(cachedOrNewConn)
		// retry if err is net.ErrClosed
		if err == nil || !errors.Is(err, net.ErrClosed) {
			return err
		}
	}

	return fn(alwaysNewConn)
}

// Dialer returns the pool's underlying dialer.
func (p *Pool) Dialer() dialer.Dialer {
	return p.connector.Dialer()
}

// ErrConnDone is returned by any operation that is performed on a connection
// that has already been returned to the connection pool.
var ErrConnDone = errors.New("pool: connection is already closed")

// Conn returns a single connection by either opening a new connection
// or returning an existing connection from the connections pool. Conn will
// block until either a connection is returned or ctx is canceled.
//
// Every Conn must be returned to the connection pool after use by
// calling Conn.Close.
func (p *Pool) Conn(ctx context.Context) (*Conn, error) {
	var pc *poolConn
	var err error

	err = p.retry(func(strategy connReuseStrategy) error {
		pc, err = p.conn(ctx, strategy)
		return err
	})
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		p:  p,
		pc: pc,
	}
	return conn, nil
}

type releaseConn func(error)

// Conn represents a single connection.
//
// A Conn must call Close to return the connection to the connections pool
// and may do so concurrently with a running I/O call.
//
// After a call to Close, all operations on the
// connection fail with ErrConnDone.
type Conn struct {
	p *Pool

	// closemu prevents the connection from closing while there
	// is an active I/O operation. It is held for read during I/O calls
	// and exclusively during close.
	closemu sync.RWMutex

	// pc is owned until close, at which point
	// it's returned to the connection pool.
	pc *poolConn

	// done transitions from false to true exactly once, on close.
	// Once done, all operations fail with ErrConnDone.
	done atomic.Bool
}

// grabConn takes a context to implement connGrabber
// but the context is not used.
func (c *Conn) grabConn(context.Context) (*poolConn, releaseConn, error) {
	if c.done.Load() {
		return nil, nil, ErrConnDone
	}

	c.closemu.RLock()
	return c.pc, c.closemuRUnlockCondReleaseConn, nil
}

// PingContext verifies the connection is still alive.
func (c *Conn) PingContext(ctx context.Context) error {
	pc, release, err := c.grabConn(ctx)
	if err != nil {
		return err
	}
	return pc.ping(ctx, release)
}

// Ping verifies the connection is still alive.
//
// Ping uses context.Background internally; to specify the context, use
// PingContext.
func (c *Conn) Ping() error {
	return c.PingContext(context.Background())
}

// Read implements the net.Conn Read method.
func (c *Conn) Read(b []byte) (n int, err error) {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return 0, err
	}

	return pc.read(release, b)
}

// Write implements the net.Conn Write method.
func (c *Conn) Write(b []byte) (n int, err error) {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return 0, err
	}

	return pc.write(release, b)
}

// LocalAddr implements the net.Conn LocalAddr method.
func (c *Conn) LocalAddr() net.Addr {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return nil
	}

	return pc.localAddr(release)
}

// RemoteAddr implements the net.Conn RemoteAddr method.
func (c *Conn) RemoteAddr() net.Addr {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return nil
	}

	return pc.remoteAddr(release)
}

// SetDeadline implements the net.Conn SetDeadline method.
func (c *Conn) SetDeadline(t time.Time) error {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return err
	}

	return pc.setDeadline(release, t)
}

// SetReadDeadline implements the net.Conn SetReadDeadline method.
func (c *Conn) SetReadDeadline(t time.Time) error {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return err
	}

	return pc.setReadDeadline(release, t)
}

// SetWriteDeadline implements the net.Conn SetWriteDeadline method.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	pc, release, err := c.grabConn(c.ctx())
	if err != nil {
		return err
	}

	return pc.setWriteDeadline(release, t)
}

// Raw executes f exposing the underlying dialer connection for the
// duration of f. The dialerConn must not be used outside of f.
//
// Once f returns and err is not net.ErrClosed, the Conn will continue to be usable
// until Conn.Close is called.
func (c *Conn) Raw(f func(dialerConn any) error) (err error) {
	var pc *poolConn
	var release releaseConn

	// grabConn takes a context to implement stmtConnGrabber, but the context is not used.
	pc, release, err = c.grabConn(c.ctx())
	if err != nil {
		return
	}

	fPanic := true
	pc.Mutex.Lock()
	defer func() {
		pc.Mutex.Unlock()

		// If f panics fPanic will remain true.
		// Ensure an error is passed to release so the connection
		// may be discarded.
		if fPanic {
			err = net.ErrClosed
		}
		release(err)
	}()

	err = f(pc.ci)
	fPanic = false
	return
}

// closemuRUnlockCondReleaseConn read unlocks closemu
// as the I/O operation is done with the pc.
func (c *Conn) closemuRUnlockCondReleaseConn(err error) {
	c.closemu.RUnlock()
	if errors.Is(err, net.ErrClosed) {
		c.close(err)
	}
}

func (c *Conn) ctx() context.Context {
	return nil
}

func (c *Conn) close(err error) error {
	if !c.done.CompareAndSwap(false, true) {
		return ErrConnDone
	}

	// Lock around releasing the pool connection
	// to ensure all reads have been stopped before doing so.
	c.closemu.Lock()
	defer c.closemu.Unlock()

	c.pc.releaseConn(err)
	c.pc = nil
	c.p = nil
	return err
}

// Close returns the connection to the connection pool.
// All operations after a Close will return with ErrConnDone.
// Close is safe to call concurrently with other operations and will
// block until all other operations finish. It may be useful to first
// cancel any used context and then call close directly after.
func (c *Conn) Close() error {
	return c.close(nil)
}

// connGrabber represents a Conn that will return the underlying
// poolConn and release function.
type connGrabber interface {
	// grabConn returns the poolConn and the associated release function
	// that must be called when the operation completes.
	grabConn(context.Context) (*poolConn, releaseConn, error)

	// ctx returns the context if available.
	// The returned context should be selected on along with
	// any read context when awaiting a cancel.
	ctx() context.Context
}

var _ connGrabber = &Conn{}

func stack() string {
	var buf [2 << 10]byte
	return string(buf[:runtime.Stack(buf[:], false)])
}

// withLock runs while holding lk.
func withLock(l sync.Locker, fn func()) {
	l.Lock()
	defer l.Unlock() // in case fn panics
	fn()
}
