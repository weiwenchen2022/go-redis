// Copyright 2012 Gary Burd
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

package redis

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestScript(t *testing.T) {
	var ctx = context.Background()
	c, err := DialDefaultServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Error(err)
	}

	const script = "return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}"
	s := NewScript(script)

	if exists, err := s.Exists(ctx, c).Bools(); err != nil || exists[0] {
		t.Error("script flush failed")
	}

	keys := []string{"key1", "key2"}
	args := []string{"arg1", "arg2"}
	want := append(keys, args...)

	r := s.Run(ctx, c, keys, Args{}.AddFlat(args)...)
	if err := r.Err(); err != nil {
		t.Errorf("s.Run(%#v, %#v...) returned %v", keys, args, err)
	} else if got, err := r.Strings(); err != nil || !cmp.Equal(want, got) {
		t.Errorf("s.Run(%#v, %#v...) got %#v, %v, want %#v, nil", keys, args, got, err, want)
	}

	if err := s.Load(ctx, c).Err(); err != nil {
		t.Errorf("s.Load() returned %v", err)
	}

	if exists, err := s.Exists(ctx, c).Bools(); err != nil || !exists[0] {
		t.Error("script unexpected flush")
	}

	r = s.RunRO(ctx, c, keys, Args{}.AddFlat(args)...)
	if err := r.Err(); err != nil {
		t.Errorf("s.RunRO(%v, %v...) returned %v", keys, args, err)
	} else if got, err := r.Strings(); err != nil || !cmp.Equal(want, got) {
		t.Errorf("s.RunRO(%v, %v...) got %#v, %v, want %#v, nil", keys, args, got, err, want)
	}
}
