package redis

import (
	"context"
	"net"
	"strings"

	"github.com/gomodule/redigo/redis"
)

// hasErrorPrefix checks if the err is a Redis error and the message contains a prefix.
func hasErrorPrefix(err error, prefix string) bool {
	err, ok := err.(redis.Error)
	if !ok {
		return false
	}
	return strings.HasPrefix(err.Error(), prefix)
}

func isBadConn(err error, allowTimeout bool) bool {
	switch err {
	case nil:
		return false
	case context.Canceled, context.DeadlineExceeded:
		return true
	}

	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}
	return true
}
