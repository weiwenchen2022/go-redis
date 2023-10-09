// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	serverPath     = flag.String("redis-server", "redis-server", "Path to redis server binary")
	serverAddress  = flag.String("redis-address", "localhost", "The address of the server")
	serverBasePort = flag.Int("redis-port", 16379, "Beginning of port range for test servers")
	serverLogName  = flag.String("redis-log", "", "Write Redis server logs to `filename`")

	serverLog = io.Discard

	defaultServerMu  sync.Mutex
	defaultServer    *Server
	defaultServerErr error
)

type Server struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
}

func NewServer(name string, args ...string) (*Server, error) {
	s := &Server{
		name: name,
		cmd:  exec.Command(*serverPath, args...),
		done: make(chan struct{}),
	}

	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = s.cmd.Start()
	if err != nil {
		return nil, err
	}

	ready := make(chan error, 1)
	go s.watch(stdout, ready)

	select {
	case err = <-ready:
	case <-time.After(10 * time.Second):
		err = errors.New("timeout waiting for server to start")
	}

	if err != nil {
		s.Stop()
		return nil, err
	}

	return s, nil
}

func (s *Server) watch(r io.Reader, ready chan<- error) {
	fmt.Fprintf(serverLog, "%d START %s\n", s.cmd.Process.Pid, s.name)

	listening := false
	text := ""
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		text = sc.Text()
		fmt.Fprintln(serverLog, text)

		if !listening {
			switch {
			case strings.Contains(text, " * Ready to accept connections"):
				fallthrough
			case strings.Contains(text, " * The server is now ready to accept connections on port"):
				listening = true
				ready <- nil
			}
		}
	}

	if !listening {
		ready <- fmt.Errorf("server exited: %s", text)
	}
	if err := s.cmd.Wait(); err != nil {
		if listening {
			ready <- err
		}
	}
	fmt.Fprintf(serverLog, "%d STOP %s\n", s.cmd.Process.Pid, s.name)
	close(s.done)
}

func (s *Server) Stop() {
	_ = s.cmd.Process.Signal(os.Interrupt)
	<-s.done
}

// stopDefaultServer stops the server created by DialDefaultServer.
func stopDefaultServer() {
	defaultServerMu.Lock()
	defer defaultServerMu.Unlock()
	if defaultServer != nil {
		defaultServer.Stop()
		defaultServer = nil
	}
}

// DefaultServerAddr starts the test server if not already started
// and returns the address of that server.
func DefaultServerAddr() (string, error) {
	defaultServerMu.Lock()
	defer defaultServerMu.Unlock()
	addr := fmt.Sprintf("%s:%d", *serverAddress, *serverBasePort)
	if defaultServer != nil || defaultServerErr != nil {
		return addr, defaultServerErr
	}

	defaultServer, defaultServerErr = NewServer(
		"default",
		"--port", strconv.Itoa(*serverBasePort),
		"--bind", *serverAddress,
		"--save", "",
		"--appendonly", "no",
	)
	return addr, defaultServerErr
}

// DialDefaultServer starts the test server if not already started and dials a
// connection to the server.
func DialDefaultServer(ctx context.Context) (*Conn, error) {
	addr, err := DefaultServerAddr()
	if err != nil {
		return nil, err
	}

	var options []redis.DialOption
	options = append(options,
		redis.DialConnectTimeout(5*time.Second),
		redis.DialReadTimeout(3*time.Second),
		redis.DialWriteTimeout(3*time.Second),
	)
	c, err := redis.Dial("tcp", addr, options...)
	if err != nil {
		return nil, err
	}

	if _, err := c.Do("FLUSHDB"); err != nil {
		return nil, err
	}
	return &Conn{c}, nil
}

func TestMain(m *testing.M) {
	os.Exit(func() int {
		flag.Parse()

		if *serverLogName != "" {
			f, err := os.OpenFile(*serverLogName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening redis-log: %v\n", err)
				return 1
			}
			defer f.Close()
			serverLog = f
		}

		defer stopDefaultServer()

		return m.Run()
	}())
}
