package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
)

// newTestChangeStream returns a change stream that opens against handler, in
// process. A nil handler is for tests that never open the stream.
func newTestChangeStream(s *Session, handler http.Handler) *changeStream {
	var client *http.Client
	if handler != nil {
		client = serveHTTP(handler).client()
	}
	return &changeStream{
		session:         s,
		endpoint:        testEndpoint,
		httpClient:      client,
		sessionID:       "session-1",
		protocolVersion: "2025-11-25",
		maxRetries:      1,
		maxEventSize:    mcp.DefaultMaxEventSize,
	}
}

// awaitResponse registers a call the session waits on, as call does.
func awaitResponse(s *Session, key string) chan *jsonrpc.Response {
	ch := make(chan *jsonrpc.Response, 1)
	s.pending[key] = &pendingCall{method: methodCallTool, response: ch}
	return ch
}

func TestReadEventsFollowsTheEventStreamFormat(t *testing.T) {
	s, _ := bareSession(t)
	first, other, unfinished := awaitResponse(s, "1"), awaitResponse(s, "2"), awaitResponse(s, "3")
	c := newTestChangeStream(s, nil)

	body := ": a comment\r\n" +
		"id: e1\r\n" +
		// One event's data over two lines, with CRLF line ends.
		"data: {\"jsonrpc\":\"2.0\",\r\n" +
		"data: \"id\":\"1\",\"result\":{}}\r\n" +
		"\r\n" +
		// An event of another name is not a message.
		"event: other\n" +
		"data: {\"jsonrpc\":\"2.0\",\"id\":\"2\",\"result\":{}}\n" +
		"\n" +
		// An id holding NUL is ignored; retry sets the wait; a field with no
		// colon has an empty value.
		"id: bad\x00id\n" +
		"retry: 1500\n" +
		"data\n" +
		"\n" +
		// An event the stream ends before finishing is dropped.
		"data: {\"jsonrpc\":\"2.0\",\"id\":\"3\",\"result\":{}}\n"

	heard, err := c.readEvents(strings.NewReader(body))
	if !errors.Is(err, io.EOF) || !heard {
		t.Fatalf("readEvents = %v, %v; want heard and io.EOF", heard, err)
	}
	select {
	case <-first:
	default:
		t.Error("the event split over two data lines was not delivered")
	}
	select {
	case <-other:
		t.Error("an event named other was delivered")
	case <-unfinished:
		t.Error("an unfinished event was delivered")
	default:
	}
	if c.lastEventID != "e1" {
		t.Errorf("last event ID is %q; want e1", c.lastEventID)
	}
	if c.serverRetry != 1500*time.Millisecond {
		t.Errorf("server retry is %v; want 1.5s", c.serverRetry)
	}
}

func TestReadEventsCapsTheSizeOfAnEvent(t *testing.T) {
	s, _ := bareSession(t)
	c := newTestChangeStream(s, nil)
	c.maxEventSize = 10
	if _, err := c.readEvents(strings.NewReader("data: 12345\ndata: 67890\n\n")); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("readEvents = %v; want an error for the event's size", err)
	}
}

// fakeOAuth hands out one token and counts how often it is asked to authorize.
type fakeOAuth struct {
	mu         sync.Mutex
	authorized int
}

func (f *fakeOAuth) TokenSource(context.Context) (oauth2.TokenSource, error) {
	return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token-1", TokenType: "Bearer"}), nil
}

func (f *fakeOAuth) Authorize(_ context.Context, _ *http.Request, resp *http.Response) error {
	resp.Body.Close()
	f.mu.Lock()
	f.authorized++
	f.mu.Unlock()
	return nil
}

func TestListenReadsWhatTheServerAnswers(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		sessionID string
		want      error
		reachable bool
	}{
		{"405 offers no stream", http.StatusMethodNotAllowed, "session-1", errNoStream, true},
		{"404 for the session", http.StatusNotFound, "session-1", errSessionGone, true},
		{"404 with no session", http.StatusNotFound, "", errNoStream, true},
		{"400 will not change", http.StatusBadRequest, "session-1", errNoStream, true},
		{"503 is tried again", http.StatusServiceUnavailable, "session-1", nil, false},
		{"429 is tried again", http.StatusTooManyRequests, "session-1", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := bareSession(t)
			stream := newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(c.status)
			}))
			stream.sessionID = c.sessionID

			_, err := stream.listen(context.Background())
			var failure unreachable
			if c.reachable {
				if !errors.Is(err, c.want) {
					t.Errorf("listen = %v; want %v", err, c.want)
				}
			} else if !errors.As(err, &failure) {
				t.Errorf("listen = %v; want a failure to reach the server", err)
			}
		})
	}
}

func TestListenRefusesAStreamThatIsNotAnEventStream(t *testing.T) {
	s, _ := bareSession(t)
	stream := newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, "{}")
	}))
	if _, err := stream.listen(context.Background()); !errors.Is(err, errNoStream) {
		t.Errorf("listen = %v; want errNoStream", err)
	}
}

// The stream is opened with the session, the protocol version, the last event
// ID and a token. A 401 is authorized once, and a second 401 ends the stream.
func TestListenSendsItsHeadersAndAuthorizesOnce(t *testing.T) {
	var headers http.Header
	s, _ := bareSession(t)
	oauth := &fakeOAuth{}
	stream := newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		w.WriteHeader(http.StatusUnauthorized)
	}))
	stream.oauthHandler = oauth
	stream.lastEventID = "e7"

	if _, err := stream.listen(context.Background()); !errors.Is(err, errAuthorizeAgain) {
		t.Fatalf("first listen = %v; want errAuthorizeAgain", err)
	}
	for header, want := range map[string]string{
		headerSessionID:       "session-1",
		headerProtocolVersion: "2025-11-25",
		headerLastEventID:     "e7",
		"Authorization":       "Bearer token-1",
		"Accept":              "text/event-stream",
	} {
		if got := headers.Get(header); got != want {
			t.Errorf("%s is %q; want %q", header, got, want)
		}
	}
	if _, err := stream.listen(context.Background()); !errors.Is(err, errNoStream) {
		t.Errorf("second listen = %v; want errNoStream", err)
	}
	if oauth.authorized != 1 {
		t.Errorf("authorized %d times; want 1", oauth.authorized)
	}
}

// A stream that drops is opened again with the ID of the last event heard,
// after the wait the server asked for.
func TestTheStreamReconnectsWithTheLastEventID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var lastEventIDs []string
		var opened []time.Time
		s, conn := bareSession(t)
		stream := newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			lastEventIDs = append(lastEventIDs, r.Header.Get(headerLastEventID))
			opened = append(opened, time.Now())
			n := len(opened)
			mu.Unlock()
			if n > 1 {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "retry: 250\nid: e1\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n")
		}))

		stream.run(context.Background())

		mu.Lock()
		defer mu.Unlock()
		if len(lastEventIDs) != 2 || lastEventIDs[0] != "" || lastEventIDs[1] != "e1" {
			t.Fatalf("the stream was opened with last event IDs %q; want none, then e1", lastEventIDs)
		}
		if gap := opened[1].Sub(opened[0]); gap != 250*time.Millisecond {
			t.Errorf("the stream was opened again after %v; want the 250ms the server asked for", gap)
		}
		select {
		case <-s.toolsChanged:
		default:
			t.Error("the change notification on the stream was not reported")
		}
		if conn.closed.Load() {
			t.Error("a server that offers no stream ended the session")
		}
	})
}

func TestTheStreamEndsTheSessionWhenTheServerForgetsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, conn := bareSession(t)
		newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		})).run(context.Background())
		if !conn.closed.Load() || !errors.Is(s.failure, errSessionGone) {
			t.Errorf("connection closed %v, failure %v; want closed with errSessionGone", conn.closed.Load(), s.failure)
		}
	})
}

// The session ends once the server has failed to answer more times in a row
// than the transport allows, waiting longer before each try.
func TestTheStreamEndsTheSessionWhenTheServerCannotBeReached(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var tries []time.Time
		s, conn := bareSession(t)
		stream := newTestChangeStream(s, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tries = append(tries, time.Now())
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		stream.maxRetries = 3
		stream.run(context.Background())

		if !conn.closed.Load() || s.failure == nil || !strings.Contains(s.failure.Error(), "could not be reached") {
			t.Errorf("connection closed %v, failure %v; want closed because the server could not be reached", conn.closed.Load(), s.failure)
		}
		if len(tries) != 4 {
			t.Fatalf("tried %d times; want 4, the first and three retries", len(tries))
		}
		for i, want := range []time.Duration{firstRetry, 2 * firstRetry, 4 * firstRetry} {
			if gap := tries[i+1].Sub(tries[i]); gap != want {
				t.Errorf("retry %d came after %v; want %v", i+1, gap, want)
			}
		}
	})
}
