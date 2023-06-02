// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool_test

import (
	"bytes"
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/weiwenchen2022/connpool"
	"github.com/weiwenchen2022/connpool/dialer"
)

var pool *connpool.Pool // connection pool.

type Conn struct {
	net.Conn

	Err error
}

func (c *Conn) Ping(ctx context.Context) error {
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
		return net.ErrClosed
	}

	return nil
}

func (c *Conn) Write(b []byte) (n int, err error) {
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

func (c *Conn) Read(b []byte) (n int, err error) {
	if c.Err != nil {
		return 0, c.Err
	}

	n, err = c.Conn.Read(b)
	if err != nil {
		c.Err = err
	}
	return n, err
}

func (c *Conn) IsValid() bool {
	return c.Err == nil
}

func Example_openPoolCLI() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)

	connpool.Register("dialer-name", dialer.DialerFunc(func(network, address string) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			return nil, err
		}

		return &Conn{Conn: conn}, nil
	}))

	interval := flag.Duration("interval", 1*time.Second, "poll interval")
	address := flag.String("address", os.Getenv("ADDRESS"), "connection address")
	flag.Parse()

	if len(*address) == 0 {
		log.Fatal("missing address flag")
	}

	var err error
	// Opening a dialer typically will not attempt to connect to the address.
	pool, err = connpool.New("dialer-name", "tcp", *address)
	if err != nil {
		// This will not be a connection error, but a network and address parse error or
		// another initialization error.
		log.Fatal("unable to use network and address", err)
	}
	defer pool.Close()

	pool.SetConnMaxLifetime(0)
	pool.SetMaxIdleConns(3)
	pool.SetMaxOpenConns(3)

	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)

	go func() {
		<-appSignal
		stop()
	}()

	Ping(ctx)
	Query(ctx, *interval)
}

// Ping the connection to verify network and address provided by the user is valid and the
// server accessible. If the ping fails exit the program with an error.
func Ping(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := pool.PingContext(ctx); err != nil {
		log.Fatalf("unable to connect to server: %v", err)
	}
}

// Query the server for the information requested and prints the results.
// If the query fails exit the program with an error.
func Query(ctx context.Context, d time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	conn, err := pool.Conn(ctx)
	if err != nil {
		log.Fatal("unable to execute query", err)
	}
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	var b [256]byte
	for {
		if conn == nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			conn, err = pool.Conn(ctx)
			if err != nil {
				log.Println("unable to execute query", err)
				time.Sleep(1 * time.Second)
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := conn.Write([]byte("\n")); err != nil {
				log.Println("unable to execute query", err)
				conn.Close()
				conn = nil
				continue
			}

			n, err := conn.Read(b[:])
			if err != nil {
				log.Println("unable to execute query", err)
				conn.Close()
				conn = nil
				continue
			}

			log.Printf("cur time %s", b[:n])
		}
	}
}
