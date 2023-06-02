package connpool

import (
	"context"
	"testing"
)

// Issue 9453: tests that SetMaxOpenConns can be lowered at runtime
// and affects the subsequent release of connections.
func TestMaxOpenConnsOnBusy(t *testing.T) {
	setHookpostCloseConn(func(_ *fakeConn, err error) {
		if err != nil {
			t.Errorf("Error closing fakeConn: %v", err)
		}
	})
	defer setHookpostCloseConn(nil)

	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(3)

	ctx := context.Background()

	conn0, err := p.conn(ctx, cachedOrNewConn)
	if err != nil {
		t.Fatalf("p open conn fail: %v", err)
	}

	conn1, err := p.conn(ctx, cachedOrNewConn)
	if err != nil {
		t.Fatalf("p open conn fail: %v", err)
	}

	conn2, err := p.conn(ctx, cachedOrNewConn)
	if err != nil {
		t.Fatalf("p open conn fail: %v", err)
	}

	if g, w := p.numOpen, 3; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}

	p.SetMaxOpenConns(2)
	if g, w := p.numOpen, 3; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}

	conn0.releaseConn(nil)
	conn1.releaseConn(nil)
	if g, w := p.numOpen, 2; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}

	conn2.releaseConn(nil)
	if g, w := p.numOpen, 2; w != g {
		t.Errorf("free conns = %d; want %d", g, w)
	}
}
