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

package redis_test

import (
	"context"
	"fmt"

	"github.com/weiwenchen2022/go-redis"
)

// listenPubSubChannels listens for messages on Redis pubsub channels. The
// onStart function is called after the channels are subscribed. The onMessage
// function is called for each message.
func listenPubSubChannels(ctx context.Context, rdb *redis.Client,
	onStart func() error,
	onMessage func(channel string, payload string) error,
	channels ...string,
) error {
	pubsub := rdb.Subscribe(ctx, channels...)

	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		// Go channel which receives messages.
		ch := pubsub.ChannelWithSubscriptions()

		// Consume messages.
		for msgi := range ch {
			switch msg := msgi.(type) {
			case *redis.Message:
				if err := onMessage(msg.Channel, msg.Payload); err != nil {
					done <- err
					return
				}
			case *redis.Subscription:
				switch msg.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if err := onStart(); err != nil {
						done <- err
						return
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	if err := pubsub.Unsubscribe(ctx); err != nil {
		return err
	}

	// Wait for goroutine to complete.
	return <-done
}

func publish(ctx context.Context, rdb *redis.Client) {
	if err := rdb.Do(ctx, "PUBLISH", "c1", "hello").Err(); err != nil {
		panic(err)
	}
	if err := rdb.Do(ctx, "PUBLISH", "c2", "world").Err(); err != nil {
		panic(err)
	}
	if err := rdb.Do(ctx, "PUBLISH", "c1", "goodbye").Err(); err != nil {
		panic(err)
	}
}

// This example shows how receive pubsub notifications with cancelation.
func ExamplePubSub_ChannelWithSubscriptions() {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		rdb         = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	)

	err := listenPubSubChannels(ctx, rdb,
		func() error {
			// The start callback is a good place to backfill missed
			// notifications. For the purpose of this example, a goroutine is
			// started to send notifications.
			go publish(ctx, rdb)
			return nil
		},
		func(channel string, payload string) error {
			fmt.Println("received", payload, "from", channel)

			// For the purpose of this example, cancel the listener's context
			// after receiving last message sent by publish().
			if payload == "goodbye" {
				cancel()
			}
			return nil
		},
		"c1", "c2")
	if err != nil {
		panic(err)
	}

	// Output:
	// received hello from c1
	// received world from c2
	// received goodbye from c1
}
