package connpool

import (
	"context"
	"testing"
)

func TestIssue4902(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	dialer := p.Dialer().(*fakeDialerCtx)
	opens0 := dialer.openCount

	var conn *Conn
	var err error
	for i := 0; i < 10; i++ {
		conn, err = p.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	opens := dialer.openCount - opens0
	if opens > 1 {
		t.Errorf("opens = %d; want <= 1", opens)
		t.Logf("p = %#v", p)
		t.Logf("dialer = %#v", dialer)
		t.Logf("conn = %#v", conn)
	}
}
