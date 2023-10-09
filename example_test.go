package redis_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/weiwenchen2022/go-redis"
)

var (
	ctx = context.Background()
	rdb *redis.Client
)

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:           "localhost:6379",
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	})
}

func ExampleNewClient() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	pong, err := rdb.Do(ctx, "PING").String()
	fmt.Println(pong, err)
	// Output:
	// PONG <nil>
}

func ExampleClient() {
	err := rdb.Do(ctx, "SET", "key", "value").Err()
	if err != nil {
		panic(err)
	}

	val, err := rdb.Do(ctx, "GET", "key").String()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	val2, err := rdb.Do(ctx, "GET", "missing_key").String()
	switch err {
	default:
		panic(err)
	case redis.ErrNil:
		fmt.Println("missing_key does not exist")
	case nil:
		fmt.Println("missing_key", val2)
	}
	// Output:
	// key value
	// missing_key does not exist
}

func ExampleConn() {
	conn := rdb.Conn()
	defer conn.Close()

	err := conn.Do(ctx, "CLIENT", "SETNAME", "foobar").Err()
	if err != nil {
		panic(err)
	}

	// Open other connections.
	for i := 0; i < 10; i++ {
		go rdb.Do(ctx, "Ping")
	}

	s, err := conn.Do(ctx, "CLIENT", "GETNAME").String()
	if err != nil {
		panic(err)
	}
	fmt.Println(s)
	// Output:
	// foobar
}

func ExampleResult_String() {
	err := rdb.Do(ctx, "SET", "hello", "world").Err()
	if err != nil {
		panic(err)
	}
	s, err := rdb.Do(ctx, "GET", "hello").String()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", s)
	// Output:
	// world
}

func ExampleResult_Bool() {
	err := rdb.Do(ctx, "SET", "foo", 1).Err()
	if err != nil {
		panic(err)
	}
	exists, err := rdb.Do(ctx, "EXISTS", "foo").Bool()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%t\n", exists)
	// Output:
	// true
}

func ExampleResult_Int() {
	err := rdb.Do(ctx, "SET", "k1", 1).Err()
	if err != nil {
		panic(err)
	}

	n, err := rdb.Do(ctx, "GET", "k1").Int()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", n)

	n, err = rdb.Do(ctx, "INCR", "k1").Int()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d\n", n)
	// Output:
	// 1
	// 2
}

func ExampleResult_Ints() {
	err := rdb.Do(ctx, "SADD", "set_with_integers", 4, 5, 6).Err()
	if err != nil {
		panic(err)
	}
	ints, err := rdb.Do(ctx, "SMEMBERS", "set_with_integers").Ints()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", ints)
	// Output:
	// [4 5 6]
}

func ExampleResult_Scan() {
	err := rdb.Do(ctx, "DEL", "album:1", "album:2", "album:3", "albums").Err()
	if err != nil {
		panic(err)
	}

	if _, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Send("HSET", "album:1", "title", "Red", "rating", 5)
		pipe.Send("HSET", "album:2", "title", "Earthbound", "rating", 1)
		pipe.Send("HSET", "album:3", "title", "Beat")
		pipe.Send("LPUSH", "albums", "1", "2", "3")
		return nil
	}); err != nil {
		panic(err)
	}

	res := rdb.Do(ctx, "SORT", "albums",
		"BY", "album:*->rating",
		"GET", "album:*->title",
		"GET", "album:*->rating",
	)
	if err := res.Err(); err != nil {
		panic(err)
	}

loop:
	for {
		var title string
		rating := -1 // initialize to illegal value to detect nil.
		err = res.Scan(&title, &rating)
		switch err {
		default:
			panic(err)
		case redis.ErrValuesExhausted:
			break loop
		case nil:
		}

		if rating == -1 {
			fmt.Println(title, "not-rated")
		} else {
			fmt.Println(title, rating)
		}
	}
	// Output:
	// Beat not-rated
	// Earthbound 1
	// Red 5
}

func ExampleResult_ScanSlice() {
	err := rdb.Do(ctx, "DEL", "album:1", "album:2", "album:3", "albums").Err()
	if err != nil {
		panic(err)
	}

	if _, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Send("HSET", "album:1", "title", "Red", "rating", 5)
		pipe.Send("HSET", "album:2", "title", "Earthbound", "rating", 1)
		pipe.Send("HSET", "album:3", "title", "Beat", "rating", 4)
		pipe.Send("LPUSH", "albums", "1", "2", "3")
		return nil
	}); err != nil {
		panic(err)
	}

	res := rdb.Do(ctx, "SORT", "albums",
		"BY", "album:*->rating",
		"GET", "album:*->title",
		"GET", "album:*->rating",
	)
	if err := res.Err(); err != nil {
		panic(err)
	}

	var albums []struct {
		Title  string
		Rating int
	}
	if err := res.ScanSlice(&albums); err != nil {
		panic(err)
	}
	fmt.Printf("%v\n", albums)
	// Output:
	// [{Earthbound 1} {Beat 4} {Red 5}]
}

func ExampleResult_ScanStruct() {
	err := rdb.Do(ctx, "DEL", "album:1").Err()
	if err != nil {
		panic(err)
	}
	if err := rdb.Do(ctx, "HSET", "album:1",
		"title", "Electric Ladyland",
		"artist", "Jimi Hendrix",
		"price", 4.95,
		"likes", 8,
	).Err(); err != nil {
		panic(err)
	}

	res := rdb.Do(ctx, "HGETALL", "album:1")
	if err := res.Err(); err != nil {
		panic(err)
	}

	type Album struct {
		Title  string  `redis:"title"`
		Artist string  `redis:"artist"`
		Price  float64 `redis:"price"`
		Likes  int     `redis:"likes"`
	}

	ab := new(Album)
	if err := res.ScanStruct(ab); err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", ab)
	// Output:
	// &{Title:Electric Ladyland Artist:Jimi Hendrix Price:4.95 Likes:8}
}

func ExampleClient_Pipelined() {
	err := rdb.Do(ctx, "DEL", "pipelined_counter").Err()
	if err != nil {
		panic(err)
	}

	var incr *redis.Result
	_, err = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		incr = pipe.Send("INCR", "pipelined_counter")
		pipe.Send("EXPIRE", "pipelined_counter", 1*time.Minute)
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(incr.Int())
	// Output: 1 <nil>
}

func ExampleClient_Pipeline() {
	err := rdb.Do(ctx, "DEL", "pipeline_counter").Err()
	if err != nil {
		panic(err)
	}

	pipe := rdb.Pipeline()

	incr := pipe.Send("INCR", "pipeline_counter")
	pipe.Send("EXPIRE", "pipeline_counter", 1*time.Minute)

	// Execute
	//
	//	INCR pipeline_counter
	//	EXPIRE pipeline_counts 60
	//
	// using one rdb-server roundtrip.
	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(incr.Int())
	// Output: 1 <nil>
}

func ExampleClient_TxPipelined() {
	err := rdb.Do(ctx, "DEL", "tx_pipelined_counter").Err()
	if err != nil {
		panic(err)
	}

	var incr *redis.Result
	_, err = rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		incr = pipe.Send("INCR", "tx_pipelined_counter")
		pipe.Send("EXPIRE", "tx_pipelined_counter", 1*time.Minute)
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(incr.Int())
	// Output: 1 <nil>
}

func ExampleClient_TxPipeline() {
	err := rdb.Do(ctx, "DEL", "tx_pipeline_counter").Err()
	if err != nil {
		panic(err)
	}

	pipe := rdb.TxPipeline()

	incr := pipe.Send("INCR", "tx_pipeline_counter")
	pipe.Send("EXPIRE", "tx_pipeline_counter", 1*time.Minute)

	// Execute
	//
	//	MULTI
	//	INCR tx_pipeline_counter
	//	EXPIRE tx_pipeline_counter 60
	//	EXEC
	//
	// using one rdb-server roundtrip.
	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(incr.Int())
	// Output: 1 <nil>
}

func ExampleClient_Watch() {
	// Increment transactionally increments key using GET and SET commands.
	increment := func(key string) error {
		// Transactional function.
		txf := func(tx *redis.Tx) error {
			// Get current value or zero.
			n, err := tx.Do(ctx, "GET", key).Int()
			if err != nil && redis.ErrNil != err {
				return err
			}

			// Actual opperation (local in optimistic lock).
			n++

			// Operation is committed only if the watched keys remain unchanged.
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Send("SET", key, n)
				return nil
			})
			return err
		}

		const maxRetries = 10000
		for i := 0; i < maxRetries; i++ {
			switch err := rdb.Watch(ctx, txf, key); err {
			default:
				// Return any other error.
				return err
			case nil:
				// Success.
				return nil
			case redis.ErrNil:
				// Optimistic lock lost. Retry.
			}
		}
		return errors.New("increment reached maximum number of retries")
	}

	err := rdb.Do(ctx, "DEL", "counter3").Err()
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if err := increment("counter3"); err != nil {
				fmt.Println("increment error:", err)
			}
		}()
	}
	wg.Wait()

	n, err := rdb.Do(ctx, "GET", "counter3").Int()
	fmt.Println("ended with", n, err)
	// Output: ended with 100 <nil>
}

func ExamplePubSub() {
	pubsub := rdb.Subscribe(ctx, "mychannel1")

	// Wait for confirmation that subscription is created before publishing anything.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		panic(err)
	}

	// Go channel which receives messages.
	ch := pubsub.Channel()

	// Publish a message.
	err = rdb.Do(ctx, "PUBLISH", "mychannel1", "hello").Err()
	if err != nil {
		panic(err)
	}

	time.AfterFunc(1*time.Second, func() {
		// When pubsub is closed channel is closed too.
		_ = pubsub.Close()
	})

	// Consume messages.
	for msg := range ch {
		fmt.Println(msg.Channel, msg.Payload)
	}
	// Output: mychannel1 hello
}

func ExamplePubSub_Receive() {
	pubsub := rdb.Subscribe(ctx, "mychannel2")
	defer pubsub.Close()

	for i := 0; i < 2; i++ {
		// ReceiveTimeout is a low level API. Use ReceiveMessage instead.
		msgi, err := pubsub.ReceiveTimeout(1 * time.Second)
		if err != nil {
			panic(err)
		}

		switch msg := msgi.(type) {
		case *redis.Subscription:
			fmt.Println("subscribed to", msg.Channel)

			_, err := rdb.Do(ctx, "PUBLISH", "mychannel2", "hello").Int()
			if err != nil {
				panic(err)
			}
		case *redis.Message:
			fmt.Println("received", msg.Payload, "from", msg.Channel)
		default:
			panic("unreached")
		}
	}
	// Output:
	// subscribed to mychannel2
	// received hello from mychannel2
}

func ExampleScript() {
	incrByXX := redis.NewScript(`
		if not redis.call("GET", KEYS[1]) then
			return false
		end
		return redis.call("INCRBY", KEYS[1], ARGV[1])
	`)

	if err := rdb.Do(ctx, "DEL", "xx_counter").Err(); err != nil {
		panic(err)
	}

	n, err := incrByXX.Run(ctx, rdb, []string{"xx_counter"}, 2).Int()
	fmt.Println(n, err)

	err = rdb.Do(ctx, "SET", "xx_counter", "40").Err()
	if err != nil {
		panic(err)
	}

	n, err = incrByXX.Run(ctx, rdb, []string{"xx_counter"}, 2).Int()
	fmt.Println(n, err)
	// Output:
	// 0 redis: nil
	// 42 <nil>
}
