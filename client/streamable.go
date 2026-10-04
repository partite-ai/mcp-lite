package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	headerProtocolVersion = "MCP-Protocol-Version"
	headerSessionID       = "Mcp-Session-Id"
	headerLastEventID     = "Last-Event-ID"

	// defaultMaxRetries is how many times in a row the GET stream may fail to
	// reach the server before the session ends, when the transport's
	// MaxRetries is zero. It matches the SDK's transport.
	defaultMaxRetries = 5
)

// streamableRoundTripper watches the requests of a streamable HTTP connection.
//
// It sets MCP-Protocol-Version on every request once the handshake has agreed
// on a version. The SDK's transport sets the header itself from the request
// metadata of a stateless call, and otherwise only for the SDK's own client,
// so this covers the initialize handshake's protocol and every notification.
//
// It also keeps the status of the last POST, which tells a server that refused
// server/discover with a plain HTTP error from one that could not be reached.
// Only discover is in flight when the handshake reads it.
type streamableRoundTripper struct {
	inner          http.RoundTripper
	version        atomic.Value // string
	lastPostStatus atomic.Int32
}

// wrap returns a copy of client whose requests go through the round tripper.
func (rt *streamableRoundTripper) wrap(client *http.Client) *http.Client {
	var copied http.Client
	if client != nil {
		copied = *client
	}
	rt.inner = copied.Transport
	if rt.inner == nil {
		rt.inner = http.DefaultTransport
	}
	copied.Transport = rt
	return &copied
}

// setProtocolVersion is a no-op on a nil round tripper, which a session over
// another transport has.
func (rt *streamableRoundTripper) setProtocolVersion(version string) {
	if rt != nil {
		rt.version.Store(version)
	}
}

// lastPostRefused reports whether the server answered the last POST with an
// HTTP status that refuses the request, rather than one that says to try again
// later. It is false on a nil round tripper.
func (rt *streamableRoundTripper) lastPostRefused() bool {
	if rt == nil {
		return false
	}
	status := int(rt.lastPostStatus.Load())
	return status >= 400 && status < 500 && !retryableStatus(status)
}

func (rt *streamableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	version, _ := rt.version.Load().(string)
	if version != "" && req.Header.Get(headerProtocolVersion) == "" {
		req = req.Clone(req.Context())
		req.Header.Set(headerProtocolVersion, version)
	}
	isPost := req.Method == http.MethodPost
	if isPost {
		rt.lastPostStatus.Store(0)
	}
	resp, err := rt.inner.RoundTrip(req)
	if isPost && err == nil {
		rt.lastPostStatus.Store(int32(resp.StatusCode))
	}
	return resp, err
}

// retryableStatus reports whether an HTTP status says the request may succeed
// if sent again later.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooManyRequests:
		return true
	}
	return status >= 500
}

// changeStream holds open the GET stream on which a streamable HTTP server
// before 2026-07-28 sends what does not answer a request, such as a change to
// its tools. The SDK's transport opens this stream only for the SDK's own
// client.
//
// Since nothing else watches the connection between requests, the stream is
// also how the session learns that the server is gone: a 404 for the session,
// or MaxRetries failures in a row to reach the server, end the session.
type changeStream struct {
	session         *Session
	endpoint        string
	httpClient      *http.Client
	oauthHandler    auth.OAuthHandler
	sessionID       string
	protocolVersion string
	maxRetries      int
	maxEventSize    int // 0 means no cap

	lastEventID string
	// serverRetry is the wait the server asked for with a retry field. It is
	// zero until the server asks.
	serverRetry time.Duration
	// authorized is set once the OAuth handler has been asked to authorize
	// the stream, and cleared once the stream opens.
	authorized bool

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newChangeStream(s *Session, transport *mcp.StreamableClientTransport, sessionID, protocolVersion string) *changeStream {
	ctx, cancel := context.WithCancel(s.ctx)
	c := &changeStream{
		session:         s,
		endpoint:        transport.Endpoint,
		httpClient:      transport.HTTPClient,
		oauthHandler:    transport.OAuthHandler,
		sessionID:       sessionID,
		protocolVersion: protocolVersion,
		maxRetries:      transport.MaxRetries,
		maxEventSize:    transport.MaxEventSize,
		cancel:          cancel,
	}
	if c.maxRetries == 0 {
		c.maxRetries = defaultMaxRetries
	} else if c.maxRetries < 0 {
		c.maxRetries = 0
	}
	if c.maxEventSize == 0 {
		c.maxEventSize = mcp.DefaultMaxEventSize
	} else if c.maxEventSize < 0 {
		c.maxEventSize = 0
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.run(ctx)
	}()
	return c
}

func (c *changeStream) stop() {
	c.cancel()
	c.wg.Wait()
}

// What one opening of the stream came to.
var (
	// errNoStream means the server offers no stream, or refuses this one in a
	// way that will not change. The session goes on without it.
	errNoStream = errors.New("the server offers no stream of notifications")

	// errSessionGone means the server no longer knows the session.
	errSessionGone = errors.New("the server no longer knows the session")

	// errAuthorizeAgain means the OAuth handler authorized the stream, which
	// should be opened again at once.
	errAuthorizeAgain = errors.New("authorized; open the stream again")
)

// unreachable is a failure to reach the server: a network error or a status
// that may change if asked again.
type unreachable struct{ err error }

func (u unreachable) Error() string { return u.err.Error() }
func (u unreachable) Unwrap() error { return u.err }

// run opens the stream again whenever it drops, until the session ends, the
// server says it offers no stream, or the server is gone.
func (c *changeStream) run(ctx context.Context) {
	retry := firstRetry
	failures := 0
	for {
		opened := time.Now()
		heard, err := c.listen(ctx)
		if ctx.Err() != nil {
			return
		}

		var failure unreachable
		switch {
		case errors.Is(err, errNoStream):
			return
		case errors.Is(err, errSessionGone):
			c.session.fail(err)
			return
		case errors.Is(err, errAuthorizeAgain):
			continue
		case errors.As(err, &failure):
			failures++
			if failures > c.maxRetries {
				c.session.fail(fmt.Errorf("the server could not be reached %d times in a row: %w", failures, failure.err))
				return
			}
		default:
			// The stream opened and then ended. The server was reached.
			failures = 0
			if heard || time.Since(opened) >= healthyAfter {
				retry = firstRetry
			}
		}

		wait := retry
		if c.serverRetry > 0 {
			wait = c.serverRetry
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		retry = min(retry*2, lastRetry)
	}
}

// listen opens the stream once and reads it until it ends. It reports whether
// anything was heard on it.
func (c *changeStream) listen(ctx context.Context) (heard bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint, nil)
	if err != nil {
		return false, fmt.Errorf("%w: %w", errNoStream, err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(headerProtocolVersion, c.protocolVersion)
	if c.sessionID != "" {
		req.Header.Set(headerSessionID, c.sessionID)
	}
	if c.lastEventID != "" {
		req.Header.Set(headerLastEventID, c.lastEventID)
	}
	if c.oauthHandler != nil {
		tokens, err := c.oauthHandler.TokenSource(ctx)
		if err != nil {
			return false, unreachable{fmt.Errorf("get an OAuth token: %w", err)}
		}
		if tokens != nil {
			token, err := tokens.Token()
			if err != nil {
				return false, unreachable{fmt.Errorf("get an OAuth token: %w", err)}
			}
			token.SetAuthHeader(req)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, unreachable{err}
	}
	defer resp.Body.Close()

	switch status := resp.StatusCode; {
	case status == http.StatusOK:
	case status == http.StatusNotFound && c.sessionID != "":
		return false, errSessionGone
	case (status == http.StatusUnauthorized || status == http.StatusForbidden) && c.oauthHandler != nil && !c.authorized:
		c.authorized = true
		if err := c.oauthHandler.Authorize(ctx, req, resp); err != nil {
			return false, fmt.Errorf("%w: authorize: %w", errNoStream, err)
		}
		return false, errAuthorizeAgain
	case retryableStatus(status):
		return false, unreachable{fmt.Errorf("open the stream: %s", resp.Status)}
	default:
		return false, fmt.Errorf("%w: %s", errNoStream, resp.Status)
	}
	c.authorized = false

	if mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mediaType != "text/event-stream" {
		return false, fmt.Errorf("%w: the server answered with %q", errNoStream, mediaType)
	}
	return c.readEvents(resp.Body)
}

// readEvents hands each server-sent event on the stream to the session as a
// JSON-RPC message, following the event stream format: lines starting with a
// colon are comments, a field with no colon has an empty value, and an event
// that the stream ends before finishing is dropped. It reports whether
// anything was heard.
func (c *changeStream) readEvents(body io.Reader) (heard bool, err error) {
	scanner := bufio.NewScanner(body)
	// A line holds at most one event's data, so the event cap bounds it too.
	maxLine := 1 << 30
	if c.maxEventSize > 0 {
		maxLine = c.maxEventSize + len("data: ")
	}
	scanner.Buffer(make([]byte, 0, 64<<10), maxLine)

	var data bytes.Buffer
	hasData := false
	eventName := ""
	for scanner.Scan() {
		heard = true
		line := scanner.Text()
		if line == "" {
			if hasData && (eventName == "" || eventName == "message") {
				if msg, err := jsonrpc.DecodeMessage(data.Bytes()); err == nil {
					c.session.handle(msg)
				}
			}
			data.Reset()
			hasData, eventName = false, ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
			if c.maxEventSize > 0 && data.Len() > c.maxEventSize {
				return heard, fmt.Errorf("an event on the stream is larger than %d bytes", c.maxEventSize)
			}
		case "event":
			eventName = value
		case "id":
			if !strings.ContainsRune(value, 0) {
				c.lastEventID = value
			}
		case "retry":
			if ms, err := strconv.Atoi(value); err == nil && ms >= 0 {
				c.serverRetry = time.Duration(ms) * time.Millisecond
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return heard, err
	}
	return heard, io.EOF
}
