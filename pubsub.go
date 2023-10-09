package redis

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ErrClosed performs any operation on the closed client will return this error.
const ErrClosed = redis.Error("redis: client is closed")

// PubSub implements Pub/Sub commands as described in
// http://redis.io/topics/pubsub. Message receiving is NOT safe
// for concurrent use by multiple goroutines.
//
// PubSub automatically reconnects to Redis Server and resubscribes
// to the channels in case of network errors.
type PubSub struct {
	newConn func(ctx context.Context) (*Conn, error)

	mu       sync.Mutex
	cn       *Conn
	channels map[string]struct{}
	patterns map[string]struct{}

	closed bool
	exit   chan struct{}

	chOnce sync.Once
	msgCh  *channel
	allCh  *channel

	ctx context.Context
}

func newPubSub(newConn func(context.Context) (*Conn, error)) *PubSub {
	pubsub := &PubSub{
		newConn: newConn,
	}
	pubsub.init()
	return pubsub
}

func (p *PubSub) init() {
	p.exit = make(chan struct{})
}

func (p *PubSub) String() string {
	channels := mapKeys(p.channels)
	channels = append(channels, mapKeys(p.patterns)...)
	return fmt.Sprintf("PubSub(%s)", strings.Join(channels, ", "))
}

func (p *PubSub) connWithLock(ctx context.Context) (*Conn, error) {
	p.mu.Lock()
	cn, err := p.conn(ctx)
	p.mu.Unlock()
	return cn, err
}

func (p *PubSub) conn(ctx context.Context) (*Conn, error) {
	if p.closed {
		return nil, ErrClosed
	}

	if p.cn != nil {
		return p.cn, nil
	}

	cn, err := p.newConn(ctx)
	if err != nil {
		return nil, err
	}

	if err := p.resubscribe(ctx, cn); err != nil {
		_ = cn.Close()
		return nil, err
	}

	p.cn = cn
	return cn, nil
}

func (p *PubSub) writeCmd(
	ctx context.Context, cn *Conn, commandName string, args ...any,
) error {
	err := cn.Send(commandName, args...)
	if flushErr := cn.Flush(); err == nil {
		err = flushErr
	}
	return err
}

func (p *PubSub) resubscribe(ctx context.Context, cn *Conn) error {
	var firstErr error

	if len(p.channels) > 0 {
		firstErr = p._subscribe(ctx, cn, "subscribe", mapKeys(p.channels))
	}

	if len(p.patterns) > 0 {
		err := p._subscribe(ctx, cn, "psubscribe", mapKeys(p.patterns))
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func mapKeys(m map[string]struct{}) []string {
	if m == nil {
		return nil
	}
	s := make([]string, len(m))
	i := 0
	for k := range m {
		s[i] = k
		i++
	}
	return s
}

func (p *PubSub) _subscribe(
	ctx context.Context, cn *Conn, commandName string, channels []string,
) error {
	return p.writeCmd(ctx, cn, commandName, Args{}.AddFlat(channels)...)
}

func (p *PubSub) releaseConnWithLock(
	ctx context.Context,
	cn *Conn,
	err error,
	allowTimeout bool,
) {
	p.mu.Lock()
	p.releaseConn(ctx, cn, err, allowTimeout)
	p.mu.Unlock()
}

func (p *PubSub) releaseConn(
	ctx context.Context, cn *Conn, err error, allowTimeout bool,
) {
	if p.cn != cn {
		return
	}
	if isBadConn(err, allowTimeout) {
		p.reconnect(ctx, err)
	}
}

func (p *PubSub) reconnect(ctx context.Context, reason error) {
	_ = p.closeTheCn(reason)
	_, _ = p.conn(ctx)
}

func (p *PubSub) closeTheCn(reason error) error {
	if p.cn == nil {
		return nil
	}
	if !p.closed {
		log.Printf("redis: discarding bad PubSub connection: %v", reason)
	}
	err := p.cn.Close()
	p.cn = nil
	return err
}

func (p *PubSub) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrClosed
	}

	p.closed = true
	close(p.exit)
	return p.closeTheCn(ErrClosed)
}

// Subscribe the client to the specified channels. It returns
// empty subscription if there are no channels.
func (p *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.subscribe(ctx, "subscribe", channels...)
	if p.channels == nil {
		p.channels = make(map[string]struct{})
	}
	for _, s := range channels {
		p.channels[s] = struct{}{}
	}
	return err
}

// PSubscribe the client to the given patterns. It returns
// empty subscription if there are no patterns.
func (p *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.subscribe(ctx, "psubscribe", patterns...)
	if p.patterns == nil {
		p.patterns = make(map[string]struct{})
	}
	for _, s := range patterns {
		p.patterns[s] = struct{}{}
	}
	return err
}

// Unsubscribe the client from the given channels, or from all of
// them if none is given.
func (p *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(channels) > 0 {
		for _, channel := range channels {
			delete(p.channels, channel)
		}
	} else {
		// Unsubscribe from all channels.
		p.channels = nil
	}

	return p.subscribe(ctx, "unsubscribe", channels...)
}

// PUnsubscribe the client from the given patterns, or from all of
// them if none is given.
func (p *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(patterns) > 0 {
		for _, pattern := range patterns {
			delete(p.patterns, pattern)
		}
	} else {
		// Unsubscribe from all patterns.
		p.patterns = nil
	}

	return p.subscribe(ctx, "punsubscribe", patterns...)
}

func (p *PubSub) subscribe(ctx context.Context, commandName string, channels ...string) error {
	cn, err := p.conn(ctx)
	if err != nil {
		return err
	}

	err = p._subscribe(ctx, cn, commandName, channels)
	p.releaseConn(ctx, cn, err, false)
	return err
}

func (p *PubSub) Ping(ctx context.Context, payload ...string) error {
	var args []any
	if len(payload) == 1 {
		args = append(args, payload[0])
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	cn, err := p.conn(ctx)
	if err != nil {
		return err
	}

	err = p.writeCmd(ctx, cn, "PING", args...)
	p.releaseConn(ctx, cn, err, false)
	return err
}

// Subscription received after a successful subscription to channel.
type Subscription struct {
	// Can be "subscribe", "unsubscribe", "psubscribe" or "punsubscribe".
	Kind string
	// Channel name we have subscribed to.
	Channel string
	// Number of channels we are currently subscribed to.
	Count int
}

func (s *Subscription) String() string {
	return fmt.Sprintf("%s: %s", s.Kind, s.Channel)
}

// Message received as result of a PUBLISH command issued by another client.
type Message struct {
	Channel string
	Pattern string
	Payload string
}

func (m *Message) String() string {
	return fmt.Sprintf("Message<%s: %s>", m.Channel, m.Payload)
}

// Pong received as result of a PING command issued by another client.
type Pong struct {
	Payload string
}

func (p *Pong) String() string {
	if p.Payload != "" {
		return fmt.Sprintf("Pong<%s>", p.Payload)
	}
	return "Pong"
}

func (*PubSub) newMessage(reply any, err error) (any, error) {
	if err != nil {
		return nil, err
	}

	res := NewResult(reply, err)
	switch val, err := res.String(); err {
	case nil:
		return &Pong{Payload: val}, nil
	}

	switch values, err := res.Values(); err {
	case nil:
		var kind string
		values, err = redis.Scan(values, &kind)
		if err != nil {
			return nil, err
		}

		switch kind {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe":
			var s = Subscription{Kind: kind}
			if _, err := redis.Scan(values, &s.Channel, &s.Count); err != nil {
				return nil, err
			}
			return &s, nil
		case "message":
			var m Message
			if _, err := redis.Scan(values, &m.Channel, &m.Payload); err != nil {
				return nil, err
			}
			return &m, nil
		case "pmessage":
			var m Message
			if _, err := redis.Scan(values, &m.Pattern, &m.Channel, &m.Payload); err != nil {
				return nil, err
			}
			return &m, nil
		case "pong":
			var p Pong
			if _, err := redis.Scan(values, &p.Payload); err != nil {
				return nil, err
			}
			return &p, nil
		default:
			return nil, fmt.Errorf("redis: unsupported pubsub message: %q", kind)
		}
	default:
		return nil, fmt.Errorf("redis: unsupported pubsub message: %#v", reply)
	}
}

// ReceiveTimeout acts like Receive but returns an error if message
// is not received in time. This is low-level API and in most cases
// Channel should be used instead.
func (p *PubSub) ReceiveTimeout(timeout time.Duration) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Don't hold the lock to allow subscriptions and pings.

	cn, err := p.connWithLock(ctx)
	if err != nil {
		return nil, err
	}

	msgi, err := cn.ReceiveTimeout(timeout)

	p.releaseConnWithLock(ctx, cn, err, timeout > 0)

	if err != nil {
		return nil, err
	}

	return p.newMessage(msgi, err)
}

// Receive returns a message as a Subscription, Message, Pong or error.
// See PubSub example for details. This is low-level API and in most cases
// Channel should be used instead.
func (p *PubSub) Receive(ctx context.Context) (any, error) {
	// Don't hold the lock to allow subscriptions and pings.

	cn, err := p.connWithLock(ctx)
	if err != nil {
		return nil, err
	}

	msgi, err := cn.Receive(ctx)

	p.releaseConnWithLock(ctx, cn, err, false)

	if err != nil {
		return nil, err
	}

	return p.newMessage(msgi, err)
}

// ReceiveMessage returns a Message or error ignoring Subscription and Pong
// messages. This is low-level API and in most cases Channel should be used
// instead.
func (p *PubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	for {
		msgi, err := p.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msgi.(type) {
		case *Subscription, *Pong:
			// Ignore.
		case *Message:
			return msg, nil
		default:
			err := fmt.Errorf("redis: unknown message: %T", msgi)
			return nil, err
		}
	}
}

//------------------------------------------------------------------------------

// Channel returns a Go channel for concurrently receiving messages.
// The channel is closed together with the PubSub. If the Go channel
// is blocked full for 30 seconds the message is dropped.
// Receive* APIs can not be used after channel is created.
//
// go-redis periodically sends ping messages to test connection health
// and re-subscribes if ping can not not received for 30 seconds.
func (p *PubSub) Channel(opts ...ChannelOption) <-chan *Message {
	p.chOnce.Do(func() {
		p.msgCh = newChannel(p, opts...)
		p.msgCh.initMsgChan()
	})
	if p.msgCh == nil {
		panic("redis: Channel can't be called after ChannelWithSubscriptions")
	}
	return p.msgCh.msgCh
}

// ChannelWithSubscriptions is like Channel, but message type can be either
// *Subscription or *Message. Subscription messages can be used to detect
// reconnections.
//
// ChannelWithSubscriptions can not be used together with Channel.
func (p *PubSub) ChannelWithSubscriptions(opts ...ChannelOption) <-chan any {
	p.chOnce.Do(func() {
		p.allCh = newChannel(p, opts...)
		p.allCh.initAllChan()
	})
	if p.allCh == nil {
		panic("redis: ChannelWithSubscriptions can't be called after Channel")
	}
	return p.allCh.allCh
}

type ChannelOption interface {
	apply(ch *channel)
}

type channelOption func(*channel)

func (opt channelOption) apply(ch *channel) {
	opt(ch)
}

// WithChannelSize specifies the Go chan size that is used to buffer incoming messages.
//
// The default is 100 messages.
func WithChannelSize(size int) ChannelOption {
	return channelOption(func(ch *channel) {
		ch.chanSize = size
	})
}

// WithChannelHealthCheckInterval specifies the health check interval.
// PubSub will ping Redis Server if it does not receive any messages within the interval.
// To disable health check, use zero interval.
//
// The default is 3 seconds.
func WithChannelHealthCheckInterval(d time.Duration) ChannelOption {
	return channelOption(func(ch *channel) {
		ch.checkInterval = d
	})
}

// WithChannelSendTimeout specifies the channel send timeout after which
// the message is dropped.
//
// The default is 60 seconds.
func WithChannelSendTimeout(d time.Duration) ChannelOption {
	return channelOption(func(ch *channel) {
		ch.chanSendTimeout = d
	})
}

type channel struct {
	pubSub *PubSub

	chanSize        int
	chanSendTimeout time.Duration
	checkInterval   time.Duration

	msgCh chan *Message
	allCh chan any
	ping  chan struct{}
}

func newChannel(pubSub *PubSub, opts ...ChannelOption) *channel {
	ch := &channel{
		pubSub: pubSub,

		chanSize:        100,
		chanSendTimeout: 60 * time.Second,
		checkInterval:   3 * time.Second,
	}
	for _, opt := range opts {
		opt.apply(ch)
	}

	if ch.checkInterval > 0 {
		ch.initHealthCheck()
	}

	return ch
}

func (ch *channel) initHealthCheck() {
	ctx := context.Background()
	ch.ping = make(chan struct{}, 1)

	go func() {
		timer := time.NewTimer(1<<63 - 1)
		timer.Stop()

		for {
			timer.Reset(ch.checkInterval)

			select {
			case <-ch.pubSub.exit:
				return
			case <-ch.ping:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				if pingErr := ch.pubSub.Ping(ctx, ""); pingErr != nil {
					ch.pubSub.mu.Lock()
					ch.pubSub.reconnect(ctx, pingErr)
					ch.pubSub.mu.Unlock()
				}
			}
		}
	}()
}

// initMsgChan must be in sync with initAllChan.
func (ch *channel) initMsgChan() {
	ctx := context.Background()
	ch.msgCh = make(chan *Message, ch.chanSize)

	go func() {
		timer := time.NewTimer(1<<63 - 1)
		timer.Stop()

		var errCount int
		for {
			msgi, err := ch.pubSub.Receive(ctx)
			if err != nil {
				if ErrClosed == err {
					close(ch.msgCh)
					return
				}

				if errCount > 0 {
					<-time.After(100 * time.Millisecond)
				}
				errCount++
				continue
			}

			errCount = 0

			// Any message is as good as a ping.
			select {
			case ch.ping <- struct{}{}:
			default:
			}

			switch msg := msgi.(type) {
			case *Subscription, *Pong:
				// Ignore.
			case *Message:
				timer.Reset(ch.chanSendTimeout)
				select {
				case ch.msgCh <- msg:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					log.Printf(
						"redis: %v channel is full for %s (message is dropped)",
						ch, ch.chanSendTimeout)
				}
			default:
				log.Printf("redis: unknown message type: %T", msg)
			}
		}
	}()
}

// initAllChan must be in sync with initMsgChan.
func (ch *channel) initAllChan() {
	ctx := context.Background()
	ch.allCh = make(chan any, ch.chanSize)

	go func() {
		timer := time.NewTimer(1<<63 - 1)
		timer.Stop()

		var errCount int
		for {
			msgi, err := ch.pubSub.Receive(ctx)
			if err != nil {
				if ErrClosed == err {
					close(ch.allCh)
					return
				}

				if errCount > 0 {
					<-time.After(100 * time.Millisecond)
				}
				errCount++
				continue
			}

			// Any message is as good as a ping.
			select {
			case ch.ping <- struct{}{}:
			default:
			}

			switch msgi.(type) {
			case *Pong:
				// Ignore.
			case *Subscription, *Message:
				timer.Reset(ch.chanSendTimeout)
				select {
				case ch.allCh <- msgi:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
					log.Printf(
						"redis: %v channel is full for %s (message is dropped)",
						ch, ch.chanSendTimeout,
					)
				}
			default:
				log.Printf("redis: unknown message type: %T", msgi)
			}
		}
	}()
}
