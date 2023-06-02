package connpool

import "testing"

func TestErrBadConnReconnect(t *testing.T) {
	p := newTestPool(t)
	defer closePool(t, p)

	simulateBadConn := func(name string, hook *func() bool, op func() error) {
		broken, retried := false, false
		numOpen := p.numOpen

		// simulate a broken connection on the first try
		*hook = func() bool {
			if !broken {
				broken = true
				return true
			}

			retried = true
			return false
		}
		defer func() { *hook = nil }()

		if err := op(); err != nil {
			t.Errorf(name+": %v", err)
			return
		}

		if !broken || !retried {
			t.Error(name + ": Failed to simulate broken connection")
		}

		if numOpen != p.numOpen {
			t.Errorf(name+": leaked %d connection(s)!", p.numOpen-numOpen)
			numOpen = p.numOpen
		}
		_ = numOpen
	}

	// p.Ping
	pPing := func() error {
		return p.Ping()
	}
	simulateBadConn("p.Ping", &hookPingBadConn, pPing)
}
