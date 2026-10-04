package client

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// firstRetry and lastRetry bound the wait before a dropped stream of
	// notifications is opened again: a subscription, or the GET stream of an
	// earlier streamable HTTP server. The wait doubles each time.
	firstRetry = time.Second
	lastRetry  = 30 * time.Second

	// healthyAfter is how long a stream must stay open for the next drop to
	// wait only firstRetry again.
	healthyAfter = 10 * time.Second
)

// subscription keeps a subscriptions/listen call open, which is how a server
// of the stateless protocol sends notifications. The call has no answer until
// it ends; the notifications arrive while it is open.
type subscription struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newSubscription(s *Session) *subscription {
	ctx, cancel := context.WithCancel(s.ctx)
	sub := &subscription{cancel: cancel}
	sub.wg.Add(1)
	go func() {
		defer sub.wg.Done()
		sub.run(ctx, s)
	}()
	return sub
}

// run opens the subscription again whenever it ends, until the session ends
// or the server refuses it. A failure to reach the server, such as a network
// error or a 503, is tried again.
func (sub *subscription) run(ctx context.Context, s *Session) {
	params := &mcp.SubscriptionsListenParams{
		Notifications: &mcp.NotificationSubscriptions{ToolsListChanged: true},
	}
	retry := firstRetry
	for {
		opened := time.Now()
		var result json.RawMessage
		err := s.call(ctx, methodSubscriptionsListen, params, &result)
		if ctx.Err() != nil || serverRefused(err) {
			return
		}
		if time.Since(opened) >= healthyAfter {
			retry = firstRetry
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
		retry = min(retry*2, lastRetry)
	}
}

// stop cancels the call, which tells the server the subscription is over, and
// waits for the subscription to end.
func (sub *subscription) stop() {
	sub.cancel()
	sub.wg.Wait()
}
