package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Tests run in synctest bubbles, so waits, deadlines and backoff take no real
// time, and every goroutine a test starts must end before the test does. A
// bubble has no network, so HTTP servers are served in process.

// orderedSchema lists its properties out of alphabetical order, at two levels,
// so a client that read it into a Go map would give it back changed.
const orderedSchema = `{"type":"object","properties":{"zeta":{"type":"string","description":"Listed first."},"alpha":{"type":"number"},"mid":{"type":"object","properties":{"y":{"type":"boolean"},"b":{"type":"string"}}}},"required":["zeta","alpha"],"additionalProperties":false}`

// The versions the setups should negotiate: the newest of the stateless
// protocol, and the newest of the initialize handshake.
var (
	newestStateless = statelessVersions[0]
	newestHandshake = handshakeVersions[0]
)

func echo(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(req.Params.Arguments)}}}, nil
}

// newServer returns a server with two tools, one per page, so listing them
// follows a cursor. With versions set, it speaks only those.
func newServer(versions ...string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "test", Version: "1"}, &mcp.ServerOptions{PageSize: 1, SupportedProtocolVersions: versions})
	server.AddTool(&mcp.Tool{Name: "echo", Description: "Echo the arguments.", InputSchema: json.RawMessage(orderedSchema)}, echo)
	server.AddTool(&mcp.Tool{Name: "plain", InputSchema: json.RawMessage(`{"type":"object"}`)}, echo)
	return server
}

// addTool adds a tool with no arguments, numbered so each call adds a new one.
func addTool(server *mcp.Server, n int) {
	server.AddTool(&mcp.Tool{Name: "added" + strconv.Itoa(n), InputSchema: json.RawMessage(`{"type":"object"}`)}, echo)
}

// allTools reads every tool the server offers.
func allTools(s *Session) ([]*mcp.Tool, error) {
	var tools []*mcp.Tool
	for tool, err := range s.Tools(context.Background(), nil) {
		if err != nil {
			return nil, err
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

// waitTool adds a tool that runs until it is cancelled, and returns a channel
// closed when it has been.
func waitTool(server *mcp.Server) <-chan struct{} {
	cancelled := make(chan struct{})
	var once sync.Once
	server.AddTool(&mcp.Tool{Name: "wait", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			<-ctx.Done()
			once.Do(func() { close(cancelled) })
			return nil, ctx.Err()
		})
	return cancelled
}

// closeServerSessions ends the server's sessions when the test ends, so none
// is left running in the bubble. It runs after the test's own cleanups, which
// close the client first.
func closeServerSessions(t *testing.T, server *mcp.Server) {
	t.Cleanup(func() {
		for session := range server.Sessions() {
			session.Close()
		}
	})
}

// A setup is one way to reach a server: a transport and the protocol version
// the client should end up speaking over it.
type setup struct {
	name        string
	wantVersion string
	server      func() *mcp.Server
	connect     func(t *testing.T, server *mcp.Server, opts *Options) *Session
}

var setups = []setup{
	{
		name:        "in memory, stateless",
		wantVersion: newestStateless,
		server:      func() *mcp.Server { return newServer() },
		connect:     inMemory,
	},
	{
		name:        "in memory, handshake",
		wantVersion: newestHandshake,
		server:      func() *mcp.Server { return newServer(handshakeVersions...) },
		connect:     inMemory,
	},
	{
		name:        "streamable HTTP, stateless",
		wantVersion: newestStateless,
		server:      func() *mcp.Server { return newServer() },
		// In the stateless protocol a call is cancelled by ending its HTTP
		// request, which an SDK server passes on to the tool only when asked.
		connect: overHTTP(&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true}),
	},
	{
		// A stateful SDK server speaks the stateless protocol only when
		// stateless, so it refuses discover and the client falls back.
		name:        "streamable HTTP, stateful",
		wantVersion: newestHandshake,
		server:      func() *mcp.Server { return newServer() },
		connect:     overHTTP(nil),
	},
	{
		name:        "streamable HTTP, handshake only",
		wantVersion: newestHandshake,
		server:      func() *mcp.Server { return newServer(handshakeVersions...) },
		connect:     overHTTP(nil),
	},
}

// forEachSetup runs a test once for each setup, each in a bubble of its own.
func forEachSetup(t *testing.T, test func(t *testing.T, setup setup)) {
	for _, setup := range setups {
		t.Run(setup.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { test(t, setup) })
		})
	}
}

func inMemory(t *testing.T, server *mcp.Server, opts *Options) *Session {
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	closeServerSessions(t, server)
	if _, err := server.Connect(context.Background(), serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	return connect(t, clientTransport, opts)
}

func overHTTP(handlerOpts *mcp.StreamableHTTPOptions) func(t *testing.T, server *mcp.Server, opts *Options) *Session {
	return func(t *testing.T, server *mcp.Server, opts *Options) *Session {
		closeServerSessions(t, server)
		return connect(t, serveHTTP(sdkHandler(server, handlerOpts)).transport(), opts)
	}
}

// connect connects and closes the session when the test ends.
func connect(t *testing.T, transport mcp.Transport, opts *Options) *Session {
	t.Helper()
	s, err := Connect(context.Background(), transport, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// sdkHandler serves server over streamable HTTP.
func sdkHandler(server *mcp.Server, opts *mcp.StreamableHTTPOptions) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, opts)
}

// testEndpoint is the URL an in-process server is reached at. Nothing listens
// there; requests never leave the process.
const testEndpoint = "http://mcp.test/mcp"

// inProcessServer serves an http.Handler to an http.Client with no network.
// A response streams to the client through a pipe while the handler writes it,
// as an event stream needs.
type inProcessServer struct {
	handler http.Handler

	mu sync.Mutex
	// inFlight ends each response still being written.
	inFlight map[*context.CancelFunc]struct{}
}

func serveHTTP(handler http.Handler) *inProcessServer {
	return &inProcessServer{handler: handler, inFlight: map[*context.CancelFunc]struct{}{}}
}

func (s *inProcessServer) transport() *mcp.StreamableClientTransport {
	return &mcp.StreamableClientTransport{Endpoint: testEndpoint, HTTPClient: s.client()}
}

func (s *inProcessServer) client() *http.Client {
	return &http.Client{Transport: s}
}

// drop ends every response still being written, as a lost connection would.
func (s *inProcessServer) drop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for cancel := range s.inFlight {
		(*cancel)()
	}
}

func (s *inProcessServer) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancel(req.Context())
	s.mu.Lock()
	s.inFlight[&cancel] = struct{}{}
	s.mu.Unlock()

	body, bodyWriter := io.Pipe()
	w := &pipeResponseWriter{header: http.Header{}, body: bodyWriter, headerSent: make(chan struct{})}
	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.inFlight, &cancel)
			s.mu.Unlock()
			cancel()
		}()
		s.handler.ServeHTTP(w, req.Clone(ctx))
		w.WriteHeader(http.StatusOK)
		bodyWriter.Close()
	}()

	select {
	case <-w.headerSent:
	case <-ctx.Done():
		body.CloseWithError(ctx.Err())
		return nil, ctx.Err()
	}
	return &http.Response{
		Status:        strconv.Itoa(w.status) + " " + http.StatusText(w.status),
		StatusCode:    w.status,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        w.sentHeader,
		Body:          &cancelOnClose{ReadCloser: body, cancel: cancel},
		ContentLength: -1,
		Request:       req,
	}, nil
}

// pipeResponseWriter is the http.ResponseWriter of an in-process response.
type pipeResponseWriter struct {
	header     http.Header
	body       *io.PipeWriter
	once       sync.Once
	headerSent chan struct{}
	status     int
	sentHeader http.Header
}

func (w *pipeResponseWriter) Header() http.Header { return w.header }

func (w *pipeResponseWriter) WriteHeader(status int) {
	w.once.Do(func() {
		w.status = status
		w.sentHeader = w.header.Clone()
		close(w.headerSent)
	})
}

func (w *pipeResponseWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(b)
}

func (w *pipeResponseWriter) Flush() { w.WriteHeader(http.StatusOK) }

// cancelOnClose ends the handler's request when the client closes the body,
// as a server sees a client that hangs up.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// rpcRequest is the part of a JSON-RPC request body the tests look at.
type rpcRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

// readRPC reads the JSON-RPC request in the body of r, and puts the body back
// so the request can still be served.
func readRPC(r *http.Request) rpcRequest {
	body, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	var req rpcRequest
	json.Unmarshal(body, &req)
	return req
}

// writeRPCResult answers a JSON-RPC request with a result.
func writeRPCResult(w http.ResponseWriter, id json.RawMessage, result string) {
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"jsonrpc":"2.0","id":`+string(id)+`,"result":`+result+`}`)
}

// writeRPCError answers a JSON-RPC request with an error.
func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, `{"jsonrpc":"2.0","id":`+string(id)+`,"error":{"code":`+strconv.Itoa(code)+`,"message":"`+message+`"}}`)
}

// bareSession is a session with no transport behind it, for testing what
// works on a session directly, such as the change stream.
func bareSession(t *testing.T) (*Session, *fakeConn) {
	t.Helper()
	conn := &fakeConn{}
	s := &Session{
		conn:         conn,
		pending:      make(map[string]*pendingCall),
		done:         make(chan struct{}),
		toolsChanged: make(chan struct{}, 1),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	t.Cleanup(s.cancel)
	return s, conn
}

// fakeConn is an mcp.Connection that only records being closed.
type fakeConn struct {
	closed atomic.Bool
}

func (c *fakeConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (c *fakeConn) Write(context.Context, jsonrpc.Message) error { return nil }
func (c *fakeConn) Close() error                                 { c.closed.Store(true); return nil }
func (c *fakeConn) SessionID() string                            { return "" }
