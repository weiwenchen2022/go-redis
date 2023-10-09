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
	"errors"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/go-cmp/cmp"
)

var ctx = context.Background()

func expectMessage(t *testing.T, err error, pubsub *PubSub, message string, expected ...any) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}

	for i := range expected {
		actual, err := pubsub.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(expected[i], actual) {
			t.Fatal(cmp.Diff(expected[i], actual))
		}
	}
}

type connPool redis.Pool

func newConnPool(dialContext func(context.Context) (*Conn, error)) *connPool {
	return (*connPool)(&redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			cn, err := dialContext(ctx)
			if err != nil {
				return nil, err
			}
			return cn.conn, nil
		},
	})
}

func (p *connPool) newConn(ctx context.Context) (*Conn, error) {
	// c, err := (*redis.Pool)(p).GetContext(ctx)
	c, err := p.DialContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: c}, nil
}

func TestPubSub_Subscribe(t *testing.T) {
	var (
		connPool = newConnPool(DialDefaultServer)
		ctx      = context.Background()
	)

	pubsub := newPubSub(connPool.newConn)

	expectMessage(t, pubsub.Subscribe(ctx, "c1"), pubsub, "Subscribe(c1)", &Subscription{Kind: "subscribe", Channel: "c1", Count: 1})
	expectMessage(t, pubsub.Subscribe(ctx, "c2"), pubsub, "Subscribe(c2)", &Subscription{Kind: "subscribe", Channel: "c2", Count: 2})

	expectMessage(t, pubsub.PSubscribe(ctx, "p1"), pubsub, "PSubscribe(p1)", &Subscription{Kind: "psubscribe", Channel: "p1", Count: 3})
	expectMessage(t, pubsub.PSubscribe(ctx, "p2"), pubsub, "PSubscribe(p2)", &Subscription{Kind: "psubscribe", Channel: "p2", Count: 4})
	expectMessage(t, pubsub.PUnsubscribe(ctx, "p1", "p2"), pubsub, "Punsubscribe(p1, p2)",
		&Subscription{Kind: "punsubscribe", Channel: "p1", Count: 3},
		&Subscription{Kind: "punsubscribe", Channel: "p2", Count: 2},
	)

	pc, err := DialDefaultServer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	publish := func(cn *Conn, channel, message string) error {
		if err := cn.Send("PUBLISH", channel, message); err != nil {
			return err
		}
		return cn.Flush()
	}
	expectMessage(t, publish(pc, "c1", "hello"), pubsub, "PUBLISH c1 hello", &Message{Channel: "c1", Payload: "hello"})

	expectMessage(t, pubsub.Ping(ctx, "hello"), pubsub, `Ping("hello")`, &Pong{Payload: "hello"})
	expectMessage(t, pubsub.Ping(ctx), pubsub, `Send("PING")`, &Pong{})
}

func TestPubSub_ReceiveContext(t *testing.T) {
	var (
		connPool    = newConnPool(DialDefaultServer)
		ctx, cancel = context.WithCancel(context.Background())
	)

	pubsub := newPubSub(connPool.newConn)

	expectMessage(t, pubsub.Subscribe(ctx, "c1"), pubsub, "Subscribe(c1)", &Subscription{Kind: "subscribe", Channel: "c1", Count: 1})

	cancel()
	_, err := pubsub.Receive(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("recv w/canceled expected Canceled got %v", err)
	}
}

func TestPubSub_Channel(t *testing.T) {
	var (
		ctx      = context.Background()
		connPool = newConnPool(DialDefaultServer)
	)

	pubsub := newPubSub(connPool.newConn)
	if err := pubsub.Subscribe(ctx, "mychannel1"); err != nil {
		t.Fatal(err)
	}

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	pc, err := DialDefaultServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Publish a message.
	err = pc.Do(ctx, "PUBLISH", "mychannel1", "hello").Err()
	if err != nil {
		t.Fatal(err)
	}

	time.AfterFunc(1*time.Second, func() {
		// When pubsub is closed channel is closed too.
		_ = pubsub.Close()
	})

	// Consume messages.
	for msg := range ch {
		t.Log(msg.Channel, msg.Payload)
	}
}

func TestPubSub_Receive(t *testing.T) {
	var (
		ctx      = context.Background()
		connPool = newConnPool(DialDefaultServer)
	)

	pubsub := newPubSub(connPool.newConn)
	defer pubsub.Close()

	if err := pubsub.Subscribe(ctx, "mychannel2"); err != nil {
		t.Fatal(err)
	}

	pc, err := DialDefaultServer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		// ReceiveTimeout is a low level API. Use ReceiveMessage instead.
		msgi, err := pubsub.ReceiveTimeout(1 * time.Second)
		if err != nil {
			t.Fatal(err)
		}

		switch msg := msgi.(type) {
		case *Subscription:
			t.Log("subscribed to", msg.Channel)

			_, err := pc.Do(ctx, "PUBLISH", "mychannel2", "hello").Int()
			if err != nil {
				t.Fatal(err)
			}
		case *Message:
			t.Log("received", msg.Payload, "from", msg.Channel)
		default:
			t.Fatal("unreached")
		}
	}
}

func TestConnPool_newConn(t *testing.T) {
	var (
		connPool = newConnPool(DialDefaultServer)
		ctx      = context.Background()
	)

	cn, err := connPool.newConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cn.Close(); err != nil {
			t.Error(err)
		}
	}()

	pong, err := cn.Do(ctx, "PING").String()
	if err != nil || pong != "PONG" {
		t.Errorf("got %s, %v; want PONG, nil", pong, err)
	}
}
