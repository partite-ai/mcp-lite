package client

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A call whose deadline passes returns at its deadline, and the server stops
// the tool, over every transport.
func TestACallPastItsDeadlineIsCancelled(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		server := setup.server()
		cancelled := waitTool(server)
		s := setup.connect(t, server, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		started := time.Now()
		_, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "wait"})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("call returned %v; want the deadline", err)
		}
		if elapsed := time.Since(started); elapsed != time.Second {
			t.Errorf("the call returned %v after it started; want at its deadline, 1s", elapsed)
		}
		select {
		case <-cancelled:
		case <-time.After(time.Minute):
			t.Fatal("the server never stopped the tool")
		}
	})
}

// Telling the server about a cancelled call does not hold up the caller.
func TestAStalledCancelNotificationDoesNotHoldTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		waitTool(server)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && readRPC(r).Method == methodCancelled {
				select {
				case <-time.After(time.Hour):
				case <-r.Context().Done():
				}
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		started := time.Now()
		if _, err := s.CallTool(ctx, &mcp.CallToolParams{Name: "wait"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("call returned %v; want the deadline", err)
		}
		if elapsed := time.Since(started); elapsed != time.Second {
			t.Errorf("the call returned %v after it started; want at its deadline, 1s", elapsed)
		}
	})
}

// A subscription that fails to reach the server is opened again.
func TestASubscriptionIsOpenedAgainAfterAFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		closeServerSessions(t, server)
		handler := sdkHandler(server, &mcp.StreamableHTTPOptions{Stateless: true})
		var refused atomic.Bool
		changed := make(chan struct{}, 1)
		connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && readRPC(r).Method == methodSubscriptionsListen && refused.CompareAndSwap(false, true) {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			handler.ServeHTTP(w, r)
		})).transport(), &Options{ToolListChanged: func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}})

		// The first subscription is refused; the next opens after firstRetry.
		time.Sleep(firstRetry)
		synctest.Wait()
		if !refused.Load() {
			t.Fatal("the first subscription was never refused")
		}
		addTool(server, 0)
		select {
		case <-changed:
		case <-time.After(time.Minute):
			t.Fatal("no change was heard after the subscription failed once")
		}
	})
}

// Over streamable HTTP, a server that forgets the session ends it, so Wait
// returns and calls fail.
func TestASessionTheServerForgetsEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		var forgotten atomic.Bool
		srv := serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if forgotten.Load() && r.Header.Get(headerSessionID) != "" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			handler.ServeHTTP(w, r)
		}))
		s := connect(t, srv.transport(), &Options{ToolListChanged: func() {}})
		synctest.Wait()

		forgotten.Store(true)
		srv.drop()

		ended := make(chan error, 1)
		go func() { ended <- s.Wait() }()
		select {
		case err := <-ended:
			if !errors.Is(err, errSessionGone) {
				t.Errorf("Wait returned %v; want errSessionGone", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("Wait did not return after the server forgot the session")
		}
		if _, err := allTools(s); err == nil {
			t.Error("listing tools after the session ended returned no error")
		}
	})
}

// Callbacks never overlap, however fast the changes come, and none starts
// after Close.
func TestCallbacksComeOneAtATime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		var active, most, calls atomic.Int32
		s := inMemory(t, server, &Options{ToolListChanged: func() {
			now := active.Add(1)
			for {
				prev := most.Load()
				if now <= prev || most.CompareAndSwap(prev, now) {
					break
				}
			}
			time.Sleep(time.Second)
			active.Add(-1)
			calls.Add(1)
		}})

		for i := range 20 {
			addTool(server, i)
		}
		time.Sleep(time.Minute)
		if calls.Load() == 0 {
			t.Fatal("no callback ran")
		}
		if most.Load() != 1 {
			t.Errorf("%d callbacks ran at once; want 1", most.Load())
		}
		s.Close()
		after := calls.Load()
		time.Sleep(time.Minute)
		if calls.Load() != after {
			t.Error("a callback ran after Close returned")
		}
	})
}

func TestCloseFromACallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		var s *Session
		var once sync.Once
		closed := make(chan error, 1)
		s = inMemory(t, server, &Options{ToolListChanged: func() {
			once.Do(func() { closed <- s.Close() })
		}})
		addTool(server, 0)
		select {
		case err := <-closed:
			if err != nil {
				t.Errorf("Close returned %v", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("Close from a callback did not return")
		}
	})
}

func TestCloseTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := inMemory(t, newServer(), nil)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Errorf("the second Close returned %v; want nil", err)
		}
	})
}

// Close ends a call that is waiting for its result.
func TestCloseEndsACallInFlight(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		server := setup.server()
		waitTool(server)
		s := setup.connect(t, server, nil)

		result := make(chan error, 1)
		go func() {
			_, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "wait"})
			result <- err
		}()
		synctest.Wait()
		s.Close()
		select {
		case err := <-result:
			if err == nil {
				t.Error("the call in flight returned no error")
			}
		case <-time.After(time.Minute):
			t.Fatal("the call in flight did not return after Close")
		}
	})
}
