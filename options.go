package redis

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"
)

// Options keeps the settings to set up redis connection.
type Options struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string

	// host:port address.
	Addr string

	// ClientName specifies a client name to be used by the Redis server connection.
	ClientName string

	// DialContextFunc specifies a custom dial function with context for creating TCP connections.
	DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	// Username specifies the username to use when connecting to the Redis server when Redis ACLs are used.
	// Password must also be set otherwise this option will have no effect.
	Username string

	// Password specifies the password to use when connecting to the Redis server.
	Password string

	// DB specifies the database to select when dialing a connection.
	DB int

	// ConnectTimeout specifies the timeout for connecting to the Redis server.
	// Default is 5 seconds.
	ConnectTimeout time.Duration

	// ReadTimeout specifies the timeout for reading a single command reply.
	// Supported values:
	//   - `0` - default timeout (3 seconds).
	//   - `-1` - no timeout (block indefinitely).
	//   - `-2` - disables SetReadDeadline calls completely.
	ReadTimeout time.Duration

	// WriteTimeout specifies the timeout for writing a single command.
	// Supported values:
	//   - `0` - default timeout (3 seconds).
	//   - `-1` - no timeout (block indefinitely).
	//   - `-2` - disables SetWriteDeadline calls completely.
	WriteTimeout time.Duration

	// TLSConfig specifies the config to use when a TLS connection is dialed.
	TLSConfig *tls.Config

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration
}

func (opt *Options) init() {
	if opt.Addr == "" {
		opt.Addr = "localhost:6379"
	}
	if opt.Network == "" {
		if strings.HasPrefix(opt.Addr, "/") {
			opt.Network = "unix"
		} else {
			opt.Network = "tcp"
		}
	}
	if opt.ConnectTimeout == 0 {
		opt.ConnectTimeout = 5 * time.Second
	}
	switch opt.ReadTimeout {
	case -2:
		opt.ReadTimeout = -1
	case -1:
		opt.ReadTimeout = 0
	case 0:
		opt.ReadTimeout = 3 * time.Second
	}
	switch opt.WriteTimeout {
	case -2:
		opt.WriteTimeout = -1
	case -1:
		opt.WriteTimeout = 0
	case 0:
		opt.WriteTimeout = opt.ReadTimeout
	}
}
