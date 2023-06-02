// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package dialer defines interfaces to be implemented by pool
// dialers as used by package connpool.
//
// Most code should use package connpool.
//
// The dialer interface has evolved over time. Dialers should implement
// Connector and DialerContext interfaces.
// The Connector.Connect and Dialer.Dial methods should never return net.ErrClosed.
// net.ErrClosed should only be returned from Validator, or
// a io method if the connection is already in an invalid (e.g. closed) state.
//
// All Conn implementations should implement the following interfaces:
// Pinger and Validator.
//
// Before a connection is returned to the connection pool after use, IsValid is
// called if implemented.
package dialer

import (
	"context"
	"net"
)

// The DialerFunc type is an adapter to allow the use of
// ordinary functions as Dialer. If f is a function
// with the appropriate signature, DialerFunc(f) is a
// Dialer that calls f.
type DialerFunc func(network, address string) (net.Conn, error)

// Dial returns f(network, address).
func (f DialerFunc) Dial(network, address string) (net.Conn, error) {
	return f(network, address)
}

// Dialer is the interface that must be implemented by a database
// driver.
//
// Dialers may implement DialerContext for access
// to contexts and to parse the network and address only once for a pool of connections,
// instead of once per connection.
type Dialer interface {
	// Open returns a new connection to the database.
	// The name is a string in a driver-specific format.
	//
	// Open may return a cached connection (one previously
	// closed), but doing so is unnecessary; the connpool package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Dial(network, address string) (net.Conn, error)
}

// DialerContext is an optional interface that may be implemented by a Dialer.
// If a Dialer implements DialerContext, then connpool.New will call
// OpenConnector to obtain a Connector and then invoke
// that Connector's Connect method to obtain each needed connection,
// instead of invoking the Dialer's Dial method for each connection.
// The two-step sequence allows drivers to parse the network and address just once
// and also provides access to per-Conn contexts.
type DialerContext interface {
	// OpenConnector must parse the network and address in the same format that Dialer.Dial
	// parses the name parameter.
	OpenConnector(network, address string) (Connector, error)
}

// A Connector represents a dialer in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to connpool.NewPool, to allow dialers
// to implement their own connpool.Pool constructors, or returned by
// DialerContext's OpenConnector method, to allow dialers
// access to context and to avoid repeated parsing of dialer
// configuration.
//
// If a Connector implements io.Closer, the connpool package's Pool.Close
// method will call Close and return error (if any).
type Connector interface {
	// Connect returns a connection to the pool.
	// Connect may return a cached connection (one previously
	// closed), but doing so is unnecessary; the connpool package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The provided context.Context is for dialing purposes only
	// (see net.Connect) and should not be stored or used for
	// other purposes. A default timeout should still be used
	// when dialing as a connection pool may call Connect
	// asynchronously.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Connect(context.Context) (net.Conn, error)

	// Dialer returns the underlying Dialer of the Connector,
	// mainly to maintain compatibility with the Dialer method
	// on connpool.Pool.
	Dialer() Dialer
}

// Pinger is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the connpool package's Pool.Ping and
// Pool.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns net.ErrClosed, Pool.Ping and Pool.PingContext will remove
// the Conn from pool.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Validator may be implemented by Conn to allow dialers to
// signal if a connection is valid or if it should be discarded.
//
// If implemented, drivers may return the underlying error from queries,
// even if the connection should be discarded by the connection pool.
type Validator interface {
	// IsValid is called prior to placing the connection into the
	// connection pool. The connection will be discarded if false is returned.
	IsValid() bool
}
