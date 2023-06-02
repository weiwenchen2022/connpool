// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/weiwenchen2022/connpool"
	"github.com/weiwenchen2022/connpool/dialer"
)

type MyConn struct {
	net.Conn

	Err error
}

func (c *MyConn) Write(b []byte) (n int, err error) {
	if c.Err != nil {
		return 0, c.Err
	}

	if !bytes.HasSuffix(b, []byte("\n")) {
		b = append(b[:len(b):len(b)], "\n"...)
	}

	n, err = c.Conn.Write(b)
	if err != nil {
		c.Err = err
	}
	return n, err
}

func (c *MyConn) Read(b []byte) (n int, err error) {
	if c.Err != nil {
		return 0, c.Err
	}

	n, err = c.Conn.Read(b)
	if err != nil {
		c.Err = err
	}
	return n, err
}

func (c *MyConn) Ping(ctx context.Context) error {
	if c.Err != nil {
		return c.Err
	}

	if _, err := c.Conn.Write([]byte("Ping\n")); err != nil {
		c.Err = err
		return err
	}

	var b [256]byte
	if n, err := c.Conn.Read(b[:]); err != nil {
		c.Err = err
		return err
	} else if strings.Trim(string(b[:n]), "\n") != "Pong" {
		c.Err = net.ErrClosed
		return c.Err
	}

	return nil
}

func (c *MyConn) IsValid() bool {
	return c.Err == nil
}

func Example_openDBService() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)

	connpool.Register("dialer-name", dialer.DialerFunc(func(network, address string) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			return nil, err
		}

		return &MyConn{Conn: conn}, nil
	}))

	address := flag.String("address", os.Getenv("ADDRESS"), "connection address")
	flag.Parse()

	if len(*address) == 0 {
		log.Fatal("missing address flag")
	}

	// Opening a driver typically will not attempt to connect to the service.
	p, err := connpool.New("dialer-name", "tcp", *address)
	if err != nil {
		// This will not be a connection error, but a network and address parse error or
		// another initialization error.
		log.Fatal(err)
	}

	p.SetConnMaxLifetime(0)
	p.SetMaxIdleConns(50)
	p.SetMaxOpenConns(50)

	s := &Service{p: p}
	log.Fatal(http.ListenAndServe(":8080", s))
}

type Service struct {
	p *connpool.Pool
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := s.p

	switch r.URL.Path {
	default:
		http.Error(w, "not found", http.StatusNotFound)
		return
	case "/healthz":
		ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
		defer cancel()

		if err := p.PingContext(ctx); err != nil {
			http.Error(w, fmt.Sprintf("service down: %v", err), http.StatusFailedDependency)
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	case "/curtime":
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		conn, err := p.Conn(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		if _, err := conn.Write([]byte("\n")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var b [256]byte
		n, err := conn.Read(b[:])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, _ = io.WriteString(w, string(b[:n]))
		return
	}
}
