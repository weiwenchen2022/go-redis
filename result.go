package redis

import (
	"errors"

	"github.com/gomodule/redigo/redis"
)

// ErrNil reply returned by Redis when key does not exist.
const ErrNil = redis.Error("redis: nil")

type Result struct {
	reply any
	err   error

	values   []any
	valuesOK bool
}

// NewResult returns a Result initialised with reply and err.
func NewResult(reply any, err error) *Result {
	return &Result{reply: reply, err: err}
}

func (r *Result) Val() any {
	return r.reply
}

func (r *Result) Err() error {
	_, err := concreteResult(func(_ any, err error) (any, error) { return nil, err }, nil, r.err)
	return err
}

func concreteResult[R any](fn func(any, error) (R, error), v any, err error) (R, error) {
	r, err := fn(v, err)
	if redis.ErrNil == err {
		err = ErrNil
	}
	return r, err
}

func (r *Result) Int() (int, error) {
	return concreteResult(redis.Int, r.reply, r.err)
}

func (r *Result) Int64() (int64, error) {
	return concreteResult(redis.Int64, r.reply, r.err)
}

func (r *Result) Uint64() (uint64, error) {
	return concreteResult(redis.Uint64, r.reply, r.err)
}

func (r *Result) Float64() (float64, error) {
	return concreteResult(redis.Float64, r.reply, r.err)
}

func (r *Result) String() (string, error) {
	return concreteResult(redis.String, r.reply, r.err)
}

func (r *Result) Bytes() ([]byte, error) {
	return concreteResult(redis.Bytes, r.reply, r.err)
}

func (r *Result) Bool() (bool, error) {
	return concreteResult(redis.Bool, r.reply, r.err)
}

func (r *Result) Values() ([]any, error) {
	return concreteResult(redis.Values, r.reply, r.err)
}

func (r *Result) Float64s() ([]float64, error) {
	return concreteResult(redis.Float64s, r.reply, r.err)
}

func (r *Result) Strings() ([]string, error) {
	return concreteResult(redis.Strings, r.reply, r.err)
}

func (r *Result) ByteSlices() ([][]byte, error) {
	return concreteResult(redis.ByteSlices, r.reply, r.err)
}

func (r *Result) Int64s() ([]int64, error) {
	return concreteResult(redis.Int64s, r.reply, r.err)
}

func (r *Result) Ints() ([]int, error) {
	return concreteResult(redis.Ints, r.reply, r.err)
}

func (r *Result) StringMap() (map[string]string, error) {
	return concreteResult(redis.StringMap, r.reply, r.err)
}

func (r *Result) IntMap() (map[string]int, error) {
	return concreteResult(redis.IntMap, r.reply, r.err)
}

func (r *Result) Int64Map() (map[string]int64, error) {
	return concreteResult(redis.Int64Map, r.reply, r.err)
}

func (r *Result) Float64Map() (map[string]float64, error) {
	return concreteResult(redis.Float64Map, r.reply, r.err)
}

func (r *Result) Bools() ([]bool, error) {
	if values, err := concreteResult(redis.Values, r.reply, r.err); err == nil {
		s := make([]bool, len(values))
		for i, v := range values {
			s[i], err = concreteResult(redis.Bool, v, nil)
			if err != nil {
				return nil, err
			}
		}
		return s, nil
	} else {
		return nil, err
	}
}

func (r *Result) Uint64s() ([]uint64, error) {
	return concreteResult(redis.Uint64s, r.reply, r.err)
}

func (r *Result) Uint64Map() (map[string]uint64, error) {
	return concreteResult(redis.Uint64Map, r.reply, r.err)
}

func (r *Result) Positions() ([]*[2]float64, error) {
	return concreteResult(redis.Positions, r.reply, r.err)
}

var ErrValuesExhausted = errors.New("redis: values exhausted")

func (r *Result) Scan(dest ...any) error {
	if r.values == nil && !r.valuesOK {
		r.values, r.err = redis.Values(r.reply, r.err)
		r.valuesOK = true
	}

	if len(r.values) == 0 {
		return ErrValuesExhausted
	}

	var err error
	r.values, err = redis.Scan(r.values, dest...)
	return err
}

func (r *Result) ScanSlice(dest any, fieldNames ...string) error {
	values, err := redis.Values(r.reply, r.err)
	if err != nil {
		return err
	}
	return redis.ScanSlice(values, dest, fieldNames...)
}

func (r *Result) ScanStruct(dest any) error {
	values, err := redis.Values(r.reply, r.err)
	if err != nil {
		return err
	}
	return redis.ScanStruct(values, dest)
}
