// Copyright 2013 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis_test

import (
	"context"
	"fmt"

	"github.com/weiwenchen2022/go-redis"
)

// zpop pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
func zpop(ctx context.Context, rdb *redis.Client, key string) (result string, err error) {
	// Loop until transaction is successful.
	for {
		err = rdb.Watch(ctx, func(tx *redis.Tx) error {
			members, err := tx.Do(ctx, "ZRANGE", key, 0, 0).Strings()
			if err != nil {
				return err
			}
			if len(members) != 1 {
				return redis.ErrNil
			}

			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Send("ZREM", key, members[0])
				return nil
			})
			if err == nil {
				result = members[0]
			}
			return err
		}, key)
		switch err {
		default:
			// Return any other error.
			return "", err
		case nil:
			// Success.
			return result, nil
		case redis.TxFailedErr:
			// Optimistic lock lost. Retry.
		}
	}
}

// zpopScript pops a value from a ZSET.
var zpopScript = redis.NewScript(`
    local r = redis.call('ZRANGE', KEYS[1], 0, 0)
    if r ~= nil then
        r = r[1]
        redis.call('ZREM', KEYS[1], r)
    end
    return r
`)

// This example implements ZPOP as described at
// http://redis.io/topics/transactions using WATCH/MULTI/EXEC and scripting.
func Example_zpop() {
	var (
		ctx = context.Background()
		rdb = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	)

	// Add test data using a pipeline.
	if _, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, member := range []string{"red", "blue", "green"} {
			pipe.Send("ZADD", "zset", i, member)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	// Pop using WATCH/MULTI/EXEC
	v, err := zpop(ctx, rdb, "zset")
	if err != nil {
		panic(err)
	}
	fmt.Println(v)

	// Pop using a script.

	v, err = zpopScript.Run(ctx, rdb, []string{"zset"}).String()
	if err != nil {
		panic(err)
	}
	fmt.Println(v)
	// Output:
	// red
	// blue
}
