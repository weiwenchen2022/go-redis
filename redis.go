// Package redis is a client for the Redis database.
package redis

import (
	"context"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Client is a Redis client representing a pool of zero or more underlying connections.
// It's safe for concurrent use by multiple goroutines.
// Client creates and frees connections automatically; it also maintains a free pool of idle connections.
type Client struct {
	connPool *redis.Pool
}

// NewClient returns a client to the Redis Server specified by Options.
func NewClient(opt *Options) *Client {
	opt.init()

	var opts []redis.DialOption
	opts = append(opts, redis.DialConnectTimeout(opt.ConnectTimeout))
	if opt.DialContextFunc != nil {
		opts = append(opts, redis.DialContextFunc(opt.DialContextFunc))
	}

	if opt.WriteTimeout >= 0 {
		opts = append(opts, redis.DialWriteTimeout(opt.WriteTimeout))
	}
	if opt.ReadTimeout >= 0 {
		opts = append(opts, redis.DialReadTimeout(opt.ReadTimeout))
	}

	if opt.Username != "" {
		opts = append(opts, redis.DialUsername(opt.Username))
	}
	if opt.Password != "" {
		opts = append(opts, redis.DialPassword(opt.Password))
	}
	if opt.DB != 0 {
		opts = append(opts, redis.DialDatabase(opt.DB))
	}

	if opt.TLSConfig != nil {
		opts = append(opts,
			redis.DialUseTLS(true),
			redis.DialTLSConfig(opt.TLSConfig),
		)
	}

	c := &Client{
		connPool: &redis.Pool{
			DialContext: func(ctx context.Context) (redis.Conn, error) {
				return redis.DialContext(ctx, opt.Network, opt.Addr, opts...)
			},

			MaxIdle:     opt.MaxIdle,
			IdleTimeout: opt.IdleTimeout,

			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < 1*time.Minute {
					return nil
				}
				_, err := c.Do("PING")
				return err
			},
		},
	}
	return c
}

// Do sends a command to the server and returns the received reply.
// This function will use the timeout which was set when the connection is created
func (p Client) Do(ctx context.Context, commandName string, args ...any) *Result {
	c := p.Conn()
	defer c.Close()
	return c.Do(ctx, commandName, args...)
}

func (p Client) Pipeline() Pipeliner {
	return &pipeliner{c: p.Conn()}
}

func (p Client) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error) {
	return p.Pipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (p Client) TxPipeline() Pipeliner {
	return &pipeliner{c: p.Conn(), tx: true}
}

func (p Client) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error) {
	return p.TxPipeline().Pipelined(ctx, fn)
}

// Watch prepares a transaction and marks the keys to be watched
// for conditional execution if there are any keys.
//
// The transaction is automatically closed when fn exits.
func (p Client) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	c := p.Conn()
	tx := Tx{Conn: c}
	defer tx.Close(ctx)

	// log.Println("Watch", keys)

	if len(keys) > 0 {
		if err := tx.Watch(ctx, keys...).Err(); err != nil {
			log.Println("Watch", err)
			return err
		}
	}
	if err := fn(&tx); err != nil {
		return err
	}
	return nil
}

func (p Client) Eval(ctx context.Context, script string, keys []string, args ...any) *Result {
	c := p.Conn()
	defer c.Close()
	return c.Eval(ctx, script, keys, args...)
}

func (p Client) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *Result {
	c := p.Conn()
	defer c.Close()
	return c.EvalRO(ctx, script, keys, args...)
}

func (p Client) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Result {
	c := p.Conn()
	defer c.Close()
	return c.EvalSha(ctx, sha1, keys, args...)
}

func (p Client) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *Result {
	c := p.Conn()
	defer c.Close()
	return c.EvalShaRO(ctx, sha1, keys, args...)
}

func (p Client) ScriptExists(ctx context.Context, hashes ...string) *Result {
	c := p.Conn()
	defer c.Close()
	return c.ScriptExists(ctx, hashes...)
}

func (p Client) ScriptLoad(ctx context.Context, script string) *Result {
	c := p.Conn()
	defer c.Close()
	return c.ScriptLoad(ctx, script)
}

func (p Client) ScriptFlush(ctx context.Context) *Result {
	c := p.Conn()
	defer c.Close()
	return c.ScriptFlush(ctx)
}

//------------------------------------------------------------------------------

// Conn represents a single Redis connection rather than a pool of connections.
// Prefer running commands from Client unless there is a specific need
// for a continuous single Redis connection.
type Conn struct {
	conn redis.Conn
}

func (c *Conn) Do(ctx context.Context, commandName string, args ...any) *Result {
	return NewResult(redis.DoContext(c.conn, ctx, commandName, args...))
}

func (c *Conn) Send(commandName string, args ...any) error {
	return c.conn.Send(commandName, args...)
}

func (c *Conn) Flush() error {
	return c.conn.Flush()
}

func (c *Conn) Receive(ctx context.Context) (any, error) {
	return redis.ReceiveContext(c.conn, ctx)
}

func (c *Conn) ReceiveTimeout(timeout time.Duration) (any, error) {
	return redis.ReceiveWithTimeout(c.conn, timeout)
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) Err() error {
	return c.conn.Err()
}

func (c *Conn) Pipeline() Pipeliner {
	return &pipeliner{c: c}
}

func (c *Conn) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *Conn) TxPipeline() Pipeliner {
	return &pipeliner{c: c, tx: true}
}

func (c *Conn) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

func (c Conn) Eval(ctx context.Context, script string, keys []string, args ...any) *Result {
	return c.Do(ctx, "EVAL", Args{script, len(keys)}.AddFlat(keys).AddFlat(args)...)
}

func (c Conn) EvalRO(ctx context.Context, script string, keys []string, args ...any) *Result {
	return c.Do(ctx, "EVAL_RO", Args{script, len(keys)}.AddFlat(keys).AddFlat(args)...)
}

func (c Conn) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *Result {
	return c.Do(ctx, "EVALSHA", Args{sha1, len(keys)}.AddFlat(keys).AddFlat(args)...)
}

func (c Conn) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *Result {
	return c.Do(ctx, "EVALSHA_RO", Args{sha1, len(keys)}.AddFlat(keys).AddFlat(args)...)
}

func (c Conn) ScriptExists(ctx context.Context, hashes ...string) *Result {
	return c.Do(ctx, "SCRIPT", Args{"EXISTS"}.AddFlat(hashes)...)
}

func (c Conn) ScriptLoad(ctx context.Context, script string) *Result {
	return c.Do(ctx, "SCRIPT", "LOAD", script)
}

func (c Conn) ScriptFlush(ctx context.Context) *Result {
	return c.Do(ctx, "SCRIPT", "FLUSH")
}

// Conn gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p Client) Conn() *Conn {
	return &Conn{conn: p.connPool.Get()}
}

func (p Client) newConn(ctx context.Context) (*Conn, error) {
	conn, err := p.connPool.DialContext(ctx)
	// conn, err := p.connPool.GetContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (p Client) Close() error {
	return p.connPool.Close()
}

type Args = redis.Args

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
// Note that this method does not wait on a response from Redis, so the
// subscription may not be active immediately. To force the connection to wait,
// you may call the Receive() method on the returned *PubSub like so:
//
//	sub := client.Subscribe(queryResp)
//	iface, err := sub.Receive()
//	if err != nil {
//	    // handle error
//	}
//
//	// Should be *Subscription, but others are possible if other actions have been
//	// taken on sub since it was created.
//	switch iface.(type) {
//	case *Subscription:
//	    // subscribe succeeded
//	case *Message:
//	    // received first message
//	case *Pong:
//	    // pong received
//	default:
//	    // handle error
//	}
//
//	ch := sub.Channel()
func (p Client) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := p.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (p Client) PSubscribe(ctx context.Context, patterns ...string) *PubSub {
	pubsub := p.pubSub()
	if len(patterns) > 0 {
		_ = pubsub.PSubscribe(ctx, patterns...)
	}
	return pubsub
}

func (p Client) pubSub() *PubSub {
	return newPubSub(p.newConn)
}

type PoolStats = redis.PoolStats

// PoolStats returns connection pool stats.
func (p Client) PoolStats() PoolStats {
	return p.connPool.Stats()
}
