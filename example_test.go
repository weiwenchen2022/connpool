// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package connpool_test

import (
	"context"
	"log"
	"time"

	"github.com/weiwenchen2022/connpool"
)

var (
	ctx context.Context
	p   *connpool.Pool
)

func ExamplePool_PingContext() {
	// Ping and PingContext may be used to determine if communication with
	// the database server is still possible.
	//
	// When used in a command line application Ping may be used to establish
	// that further queries are possible; that the provided DSN is valid.
	//
	// When used in long running service Ping may be part of the health
	// checking system.

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	status := "up"
	if err := p.PingContext(ctx); err != nil {
		status = "down"
	}
	log.Println(status)
}
