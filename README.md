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
