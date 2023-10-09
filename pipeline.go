package redis

import (
	"context"
	"log"
)

// Pipeliner is an mechanism to realise Redis Pipeline technique.
// Pipelining is a technique to extremely speed up processing by packing operations to batches,
// send them at once to Redis and read a replies in a single step. See https://redis.io/topics/pipelining
type Pipeliner interface {
	// Len is to obtain the number of commands in the pipeline that have not yet been executed.
	Len() int

	// Send writes the command to the client's output buffer.
	Send(commandName string, args ...any) *Result

	// Discard is to discard all commands in the cache that have not yet been executed.
	Discard()

	// Exec is to send all the commands buffered in the pipeline to the redis-server.
	Exec(ctx context.Context) ([]*Result, error)

	Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error)
}

type connInterface interface {
	Close() error

	Err() error

	Do(ctx context.Context, commandName string, args ...any) *Result

	Send(commandName string, args ...any) error

	Flush() error

	Receive(ctx context.Context) (reply any, err error)
}

type pipeliner struct {
	c connInterface

	cmdAndArgs [][]any
	results    []*Result

	tx bool
}

func (p pipeliner) Len() int {
	return len(p.cmdAndArgs)
}

func (p *pipeliner) Send(commandName string, args ...any) *Result {
	p.cmdAndArgs = append(p.cmdAndArgs, append([]any{commandName}, args...))
	result := &Result{}
	p.results = append(p.results, result)
	return result
}

func (p *pipeliner) Discard() {
	p.cmdAndArgs = nil
	p.results = nil
}

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Lmicroseconds)
}

func (p *pipeliner) Exec(ctx context.Context) ([]*Result, error) {
	cmdAndArgs := p.cmdAndArgs
	p.cmdAndArgs = nil
	results := p.results
	p.results = nil

	c := p.c
	defer c.Close()

	if p.tx {
		if err := c.Send("MULTI"); err != nil {
			return nil, err
		}
	}
	for _, arg := range cmdAndArgs {
		if err := c.Send(arg[0].(string), arg[1:]...); err != nil {
			return nil, err
		}
	}
	if p.tx {
		res := c.Do(ctx, "EXEC")
		if err := res.Err(); err != nil {
			return nil, err
		}

		values, err := res.Values()
		if err != nil {
			return nil, err
		}
		for i, val := range values {
			*results[i] = *NewResult(val, nil)
		}
	} else {
		if err := c.Flush(); err != nil {
			return nil, err
		}

		for i := range results {
			*results[i] = *NewResult(c.Receive(ctx))
		}
	}
	return results, nil
}

func (p *pipeliner) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]*Result, error) {
	if err := fn(p); err != nil {
		return nil, err
	}
	return p.Exec(ctx)
}
