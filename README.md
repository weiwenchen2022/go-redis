# Redis client for Go

[![PkgGoDev](https://pkg.go.dev/badge/github.com/weiwenchen2022/go-redis)](https://pkg.go.dev/github.com/weiwenchen2022/go-redis?tab=doc)


## Features

- Automatic connection pooling with
- Print-like API with support for all Redis commands.
- Pipelines and transactions
- High-level PubSub API
- Scripting with optimistic use of EVALSHA and EVALSHA_RO.
- Reply helper type.

## Documentation

- [GoDoc](https://pkg.go.dev/github.com/weiwenchen2022/go-redis)


## Installation

go-redis supports 2 last Go versions and requires a Go version with
[modules](https://github.com/golang/go/wiki/Modules) support. So make sure to initialize a Go
module:

```shell
go mod init github.com/my/repo
```

Then install go-redis:

```shell
go get github.com/weiwenchen2022/go-redis
```

## Quickstart

```go
import (
    "context"
    "fmt"

    "github.com/weiwenchen2022/go-redis"
)

func ExampleClient() {
    var (
        ctx = context.Background()
        rdb = redis.NewClient(&redis.Options{
            Addr:     "localhost:6379",
            Password: "", // no password set
            DB:       0,  // use default DB
        })
    )

    err := rdb.Do(ctx, "SET", "key", "value").Err()
    if err != nil {
        panic(err)
    }

    val, err := rdb.Do(ctx, "GET", "key").String()
    if err != nil {
        panic(err)
    }
    fmt.Println("key", val)

    val2, err := rdb.Get(ctx, "missing_key").String()
    switch err {
    default:
        panic(err)
    case redis.Nil:
        fmt.Println("missing_key does not exist")
    case nil:
        fmt.Println("missing_key", val2)
    }
    // Output: key value
    // missing_key does not exist
}
```
