package connpool

import (
	"context"
	"testing"
)

// Test cases where there's more than maxBadConnRetries bad connections in the
// pool (issue 8834)
func TestManyErrBadConn(t *testing.T) {
	manyErrBadConnSetup := func(first ...func(p *Pool)) *Pool {
		p := newTestPool(t)

		for _, f := range first {
			f(p)
		}

		nconn := maxClosedConnRetries + 1
		p.SetMaxIdleConns(nconn)
		p.SetMaxOpenConns(nconn)

		// open enough connections
		func() {
			for i := 0; i < nconn; i++ {
				conn, err := p.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
			}
		}()

		p.mu.Lock()
		defer p.mu.Unlock()
		if nconn != p.numOpen {
			t.Fatalf("unexpected numOpen %d (was expecting %d)", p.numOpen, nconn)
		} else if nconn != len(p.freeConn) {
			t.Fatalf("unexpected len(db.freeConn) %d (was expecting %d)", len(p.freeConn), nconn)
		}

		for _, conn := range p.freeConn {
			conn.Lock()
			conn.ci.(*fakeConn).stickyBad = true
			conn.Unlock()
		}

		return p
	}

	// Conn
	p := manyErrBadConnSetup()
	defer closePool(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := p.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	// Ping
	p = manyErrBadConnSetup()
	defer closePool(t, p)
	if err := p.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
}
