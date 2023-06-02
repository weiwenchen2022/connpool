package connpool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Issue32530 encounters an issue where a connection may
// expire right after it comes out of a used connection pool
// even when a new connection is requested.
func TestConnExpiresFreshOutOfPool(t *testing.T) {
	testCases := []struct {
		expired  bool
		badReset bool
	}{
		{false, false},
		{true, false},
		{false, true},
	}

	t0 := time.Unix(1000000, 0)
	offset := time.Duration(0)
	offsetMu := sync.RWMutex{}

	nowFunc = func() time.Time {
		offsetMu.RLock()
		defer offsetMu.RUnlock()
		return t0.Add(offset)
	}
	defer func() { nowFunc = time.Now }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := newTestPool(t)
	defer closePool(t, p)

	p.SetMaxOpenConns(1)

	for _, tc := range testCases {
		tc := tc
		name := fmt.Sprintf("expired=%t,badReset=%t", tc.expired, tc.badReset)
		t.Run(name, func(t *testing.T) {
			p.clearAllConns(t)

			p.SetMaxIdleConns(1)
			p.SetConnMaxLifetime(10 * time.Second)

			conn, err := p.conn(ctx, alwaysNewConn)
			if err != nil {
				t.Fatal(err)
			}

			afterPutConn := make(chan struct{})
			waitingForConn := make(chan struct{})

			go func() {
				defer close(afterPutConn)

				conn, err := p.conn(ctx, alwaysNewConn)
				if err == nil {
					p.putConn(conn, err)
				} else {
					t.Errorf("p.conn: %v", err)
				}
			}()

			go func() {
				defer close(waitingForConn)

				for {
					if t.Failed() {
						return
					}

					p.mu.Lock()
					ct := len(p.connRequests)
					p.mu.Unlock()
					if ct > 0 {
						return
					}

					time.Sleep(pollDuration)
				}
			}()

			<-waitingForConn

			if t.Failed() {
				return
			}

			offsetMu.Lock()
			if tc.expired {
				offset = 11 * time.Second
			} else {
				offset = time.Duration(0)
			}
			offsetMu.Unlock()

			conn.ci.(*fakeConn).stickyBad = tc.badReset

			p.putConn(conn, err)

			<-afterPutConn
		})
	}
}
