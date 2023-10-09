package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"io"
)

type Scripter interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) *Result
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *Result
	EvalRO(ctx context.Context, script string, keys []string, args ...any) *Result
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *Result
	ScriptExists(ctx context.Context, hashes ...string) *Result
	ScriptLoad(ctx context.Context, script string) *Result
}

var (
	_ Scripter = (*Client)(nil)
	_ Scripter = (*Conn)(nil)
)

// Script encapsulates the source and hash for a Lua script.
type Script struct {
	src, hash string
}

func NewScript(src string) *Script {
	h := sha1.New()
	_, _ = io.WriteString(h, src)
	return &Script{
		src:  src,
		hash: hex.EncodeToString(h.Sum(nil)),
	}
}

func (s *Script) Hash() string {
	return s.hash
}

// Run optimistically uses EVALSHA to run the script. If script does not exist
// it is retried using EVAL.
func (s *Script) Run(ctx context.Context, c Scripter, keys []string, args ...any) *Result {
	r := c.EvalSha(ctx, s.hash, keys, args...)
	if hasErrorPrefix(r.Err(), "NOSCRIPT") {
		r = c.Eval(ctx, s.src, keys, args...)
	}
	return r
}

// RunRO optimistically uses EVALSHA_RO to run the script. If script does not exist
// it is retried using EVAL_RO.
func (s *Script) RunRO(ctx context.Context, c Scripter, keys []string, args ...any) *Result {
	r := c.EvalShaRO(ctx, s.hash, keys, args...)
	if hasErrorPrefix(r.Err(), "NOSCRIPT") {
		r = c.EvalRO(ctx, s.src, keys, args...)
	}
	return r
}

func (s *Script) Load(ctx context.Context, c Scripter) *Result {
	return c.ScriptLoad(ctx, s.src)
}

func (s *Script) Exists(ctx context.Context, c Scripter) *Result {
	return c.ScriptExists(ctx, s.hash)
}
