// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/weiwenchen2022/connpool/dialer"
)

// fakeDialer is a fake dialer that implements dialer.Dialer
// interface, just for testing.
type fakeDialer struct {
	mu         sync.Mutex // guards 3 following fields
	openCount  int        // conn opens
	closeCount int        // conn closes
	waitCh     chan struct{}
	waitingCh  chan struct{}

	net.Dialer
}

type fakeConnector struct {
	addr net.Addr

	waiter func(context.Context)
	closed bool
}

func (c *fakeConnector) Connect(ctx context.Context) (net.Conn, error) {
	conn, err := fdialer.Dial(c.addr.Network(), c.addr.String())
	if fc, ok := conn.(*fakeConn); ok {
		fc.waiter = c.waiter
	}

	return conn, err
}

func (c *fakeConnector) Dialer() dialer.Dialer {
	return fdialer
}

func (c *fakeConnector) Close() error {
	if c.closed {
		return errors.New("fakepool: connector is closed")
	}

	c.closed = true
	return nil
}

type fakeDialerCtx struct {
	fakeDialer

	addr     net.Addr
	addrOnce sync.Once
}

func (cc *fakeDialerCtx) NewConnector(waiter func(context.Context)) *fakeConnector {
	c, err := cc.OpenConnector("", "")
	if err != nil {
		panic(err)
	}
	fc := c.(*fakeConnector)
	fc.waiter = waiter
	return fc
}

var _ dialer.DialerContext = &fakeDialerCtx{}

func (cc *fakeDialerCtx) OpenConnector(network, address string) (dialer.Connector, error) {
	cc.addrOnce.Do(func() {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}

		cc.addr = ln.Addr()

		go func() {
			for {
				in, err := ln.Accept()
				if err != nil {
					return
				}

				go func() {
					sc := bufio.NewScanner(in)
					for sc.Scan() {
						line := sc.Text()
						args := strings.Split(line, " ")

						switch {
						case len(args) == 1 && args[0] == "Ping":
							_, _ = io.WriteString(in, "Pong\n")
							continue
						case len(args) == 2 && args[0] == "sleep":
							d, err := time.ParseDuration(args[1])
							if err != nil {
								panic(err)
							}

							time.Sleep(d)
						}

						_, _ = in.Write([]byte(line + "\n"))
					}
					_ = sc.Err()

					// _, _ = io.Copy(os.Stdout, in)
				}()
			}
		}()
	})

	return &fakeConnector{addr: cc.addr}, nil
}

type fakeError struct {
	Message string
	Wrapped error
}

func (e fakeError) Error() string {
	return e.Message
}

func (e fakeError) Unwrap() error {
	return e.Wrapped
}

type memToucher interface {
	// touchMem reads & writes some memory, to help find data races.
	touchMem()
}

type fakeConn struct {
	dialer *fakeDialer // where to return ourselves to

	net.Conn

	// Every operation writes to line to enable the race detector
	// check for data races.
	line int64

	// Stats for tests:
	mu sync.Mutex

	// bad connection tests; see isBad()
	bad       bool
	stickyBad bool
	badConn   bool

	panic string

	// The waiter is called before each query. May be used in place of the "WAIT"
	// directive.
	waiter func(context.Context)
}

func (c *fakeConn) touchMem() {
	c.line++
}

func (c *fakeConn) incrStat(v *int) {
	c.mu.Lock()
	*v++
	c.mu.Unlock()
}

var _ dialer.Pinger = (*fakeConn)(nil)

// hook to simulate broken connections
var hookPingBadConn func() bool

func (c *fakeConn) Ping(ctx context.Context) error {
	if c.panic == "Ping" {
		panic(c.panic)
	}

	if c.waiter != nil {
		c.waiter(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	if c.stickyBad || (hookPingBadConn != nil && hookPingBadConn()) {
		return fakeError{Message: "Ping: Sticky Bad", Wrapped: net.ErrClosed}
	}

	if _, err := c.Conn.Write([]byte("Ping\n")); err != nil {
		return err
	}

	var b [256]byte
	if n, err := c.Conn.Read(b[:]); err != nil {
		return err
	} else if strings.Trim(string(b[:n]), "\n") != "Pong" {
		return net.ErrClosed
	}

	return nil
}

func (c *fakeConn) SetPanic(panic string) {
	c.panic = panic
}

func (c *fakeConn) Read(b []byte) (int, error) {
	if c.panic == "Read" {
		panic(c.panic)
	}

	return c.Conn.Read(b)
}

func (c *fakeConn) Write(b []byte) (int, error) {
	if c.panic == "Write" {
		panic(c.panic)
	}

	if !bytes.HasSuffix(b, []byte("\n")) {
		b = append(b[:len(b):len(b)], "\n"...)
	}

	return c.Conn.Write(b)
}

func (c *fakeConn) LocalAddr() net.Addr {
	if c.panic == "LocalAddr" {
		panic(c.panic)
	}

	return c.Conn.LocalAddr()
}

func (c *fakeConn) RemoteAddr() net.Addr {
	if c.panic == "RemoteAddr" {
		panic(c.panic)
	}

	return c.Conn.RemoteAddr()
}

func (c *fakeConn) SetDeadline(t time.Time) error {
	if c.panic == "SetDeadline" {
		panic(c.panic)
	}

	return c.Conn.SetDeadline(t)
}

func (c *fakeConn) SetReadDeadline(t time.Time) error {
	if c.panic == "SetReadDeadline" {
		panic(c.panic)
	}

	return c.Conn.SetReadDeadline(t)
}

func (c *fakeConn) SetWriteDeadline(t time.Time) error {
	if c.panic == "SetWriteDeadline" {
		panic(c.panic)
	}

	return c.Conn.SetWriteDeadline(t)
}

var fdialer dialer.Dialer = &fakeDialerCtx{}

func init() {
	Register("test", fdialer)
}

func contains(list []string, y string) bool {
	for _, x := range list {
		if y == x {
			return true
		}
	}
	return false
}

type Dummy struct {
	dialer.Dialer
}

func TestDialers(t *testing.T) {
	unregisterAllDrivers()
	Register("test", fdialer)
	Register("invalid", Dummy{})
	all := Dialers()
	if len(all) < 2 || !sort.StringsAreSorted(all) || !contains(all, "test") || !contains(all, "invalid") {
		t.Fatalf("Dialers = %v, want sorted list with at least [invalid, test]", all)
	}
}

// hook to simulate connection failures
var hookOpenErr struct {
	sync.Mutex
	fn func() error
}

func setHookOpenErr(fn func() error) {
	hookOpenErr.Lock()
	hookOpenErr.fn = fn
	hookOpenErr.Unlock()
}

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)
}

func (d *fakeDialer) Dial(network, address string) (net.Conn, error) {
	hookOpenErr.Lock()
	fn := hookOpenErr.fn
	hookOpenErr.Unlock()
	if fn != nil {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	d.mu.Lock()
	d.openCount++
	d.mu.Unlock()

	conn, err := d.Dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}
	conn = &fakeConn{dialer: d, Conn: conn}

	if d.waitCh != nil {
		d.waitingCh <- struct{}{}
		<-d.waitCh
		d.waitCh = nil
		d.waitingCh = nil
	}

	return conn, err
}

func (c *fakeConn) isBad() bool {
	if c.stickyBad {
		return true
	} else if c.bad {
		// alternate between bad conn and not bad conn
		c.badConn = !c.badConn
		return c.badConn
	} else {
		return false
	}
}

var hookPostCloseConn struct {
	sync.Mutex
	fn func(*fakeConn, error)
}

func setHookpostCloseConn(fn func(*fakeConn, error)) {
	hookPostCloseConn.Lock()
	hookPostCloseConn.fn = fn
	hookPostCloseConn.Unlock()
}

var testStrictClose *testing.T

// setStrictFakeConnClose sets the t to Errorf on when fakeConn.Close
// fails to close. If nil, the check is disabled.
func setStrictFakeConnClose(t *testing.T) {
	testStrictClose = t
}

var _ dialer.Validator = (*fakeConn)(nil)

func (c *fakeConn) IsValid() bool {
	return !c.isBad()
}

func (c *fakeConn) Close() (err error) {
	dialer := c.dialer
	defer func() {
		if err != nil && testStrictClose != nil {
			testStrictClose.Errorf("failed to close a test fakeConn: %v", err)
		}

		hookPostCloseConn.Lock()
		fn := hookPostCloseConn.fn
		hookPostCloseConn.Unlock()
		if fn != nil {
			fn(c, err)
		}

		if err == nil {
			dialer.mu.Lock()
			dialer.closeCount++
			dialer.mu.Unlock()
		}
	}()

	c.touchMem()
	if c.dialer == nil {
		return errors.New("fakepool: can't close fakeConn; already closed")
	}
	c.dialer = nil
	return nil
}
