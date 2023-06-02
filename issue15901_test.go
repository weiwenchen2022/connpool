package connpool

import (
	"context"
	"net"
	"testing"
)

// badConn implements a bad dialer.Conn, for TestBadDriver.
// The Ping method panics.
type badConn struct {
	net.Conn
}

func (bc badConn) Close() error {
	return nil
}

func (bc badConn) Ping(context.Context) error {
	panic("badConn.Ping")
}

// badDialer is a driver.Dialer that uses badConn.
type badDialer struct{}

func (bd badDialer) Dial(network, address string) (net.Conn, error) {
	return badConn{}, nil
}

func TestBadDialer(t *testing.T) {
	Register("bad", badDialer{})
	p, err := New("bad", "ignored", "ignored")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic")
		} else {
			if want := "badConn.Ping"; want != r.(string) {
				t.Errorf("panic was %v, expected %v", r, want)
			}
		}
	}()

	p.Ping()
}
