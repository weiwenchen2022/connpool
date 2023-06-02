package connpool

import (
	"context"
	"testing"
)

// Tests fix for issue 2542, that we release a lock when querying on
// a closed connection.
func TestIssue2542Deadlock(t *testing.T) {
	p := newTestPool(t)
	closePool(t, p)

	for i := 0; i < 2; i++ {
		if _, err := p.Conn(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	}
}
