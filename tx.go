package redis

import (
	"context"

	"github.com/gomodule/redigo/redis"
)

// TxFailedErr transaction redis failed.
const TxFailedErr = redis.Error("redis: transaction failed")

// Tx implements Redis transactions as described in
// http://redis.io/topics/transactions. It's NOT safe for concurrent use
// by multiple goroutines, because Exec resets list of watched keys.
//
// If you don't need WATCH, use Pipeline instead.
type Tx struct {
	*Conn
}

func (t *Tx) Close(ctx context.Context) error {
	_ = t.Unwatch(ctx).Err()
	return t.Conn.Close()
}

func (t *Tx) Watch(ctx context.Context, keys ...string) *Result {
	return t.Do(ctx, "WATCH", Args{}.AddFlat(keys)...)
}

func (t *Tx) Unwatch(ctx context.Context, keys ...string) *Result {
	return t.Do(ctx, "UNWATCH", Args{}.AddFlat(keys)...)
}
