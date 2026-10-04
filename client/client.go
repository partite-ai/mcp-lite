// Package client is a small MCP client. It lists the tools of a server, hears
// when that list changes, and calls tools.
//
// It talks to the server over the transports of the official Go SDK and uses
// the SDK's protocol types. Unlike the SDK's own client, it keeps the schemas
// of a tool, and the structured content of a tool's result, as the bytes the
// server sent, so the order of their keys survives. The order of a schema's
// properties is part of what a model reads.
//
// Its methods match those of the SDK's ClientSession, so a caller can move to
// the SDK's client by changing how the session is made. A caller that wants
// that to stay true reads a schema by marshalling it, not by asserting that it
// is a json.RawMessage: the SDK's client holds it as a map[string]any.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	methodDiscover            = "server/discover"
	methodInitialize          = "initialize"
	methodInitialized         = "notifications/initialized"
	methodCancelled           = "notifications/cancelled"
	methodSubscriptionsListen = "subscriptions/listen"
	methodToolListChanged     = "notifications/tools/list_changed"
	methodListTools           = "tools/list"
	methodCallTool            = "tools/call"
	methodPing                = "ping"
)

// notifyTimeout bounds what the session sends on its own: telling the server
// a request was cancelled, and answering a request from the server.
const notifyTimeout = 5 * time.Second

// Options configures a session. The zero value is usable.
type Options struct {
	// ClientInfo names this client to the server.
	ClientInfo *mcp.Implementation

	// ToolListChanged is called when the server says its list of tools has
	// changed. Calls come one at a time, on a goroutine of the session's own,
	// so a call that lists the tools again never races an earlier one.
	// Changes that arrive while a call runs lead to one more call after it.
	// No call starts once Close has returned, or once the connection has
	// ended.
	ToolListChanged func()
}

// A Session is one connection to an MCP server.
type Session struct {
	conn             mcp.Connection
	opts             Options
	initializeResult *mcp.InitializeResult

	// protocolVersion is the version agreed with the server. It is set during
	// Connect, before anything but the handshake sends.
	protocolVersion string

	// http watches the requests of a streamable HTTP connection. It is nil for
	// other transports.
	http *streamableRoundTripper

	// ctx ends when the session closes or the connection ends. What the
	// session does on its own runs under it.
	ctx    context.Context
	cancel context.CancelFunc

	lastID atomic.Int64

	mu      sync.Mutex
	pending map[string]*pendingCall
	closing bool
	// failure is why the session ended itself, for a reason the connection
	// could not see, such as the server forgetting the session.
	failure error
	// err is why the connection ended. It is set before done is closed.
	err error
	// tasks counts the goroutines started with spawn. Once tasksClosed is
	// set, spawn starts none.
	tasks       sync.WaitGroup
	tasksClosed bool

	// done is closed when the connection has ended.
	done chan struct{}

	// changes keeps the server's notifications coming: a subscription in the
	// stateless protocol, or the GET stream of an earlier streamable HTTP
	// server. It is nil when nobody listens for changes.
	changes interface{ stop() }

	// toolsChanged holds a change for the callback worker to report. It has
	// room for one, so changes that come while a callback runs fold into one.
	// It is nil when there is no callback.
	toolsChanged chan struct{}
	// callbackDone is closed when the callback worker has stopped. It is nil
	// until the worker starts.
	callbackDone chan struct{}
	callbackMu   sync.Mutex
	inCallback   bool
}

// Connect connects over a transport and agrees on a protocol version with the
// server.
//
// A streamable HTTP transport is copied before it is used, so the caller's
// value is not changed.
func Connect(ctx context.Context, transport mcp.Transport, opts *Options) (*Session, error) {
	s := &Session{
		pending: make(map[string]*pendingCall),
		done:    make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if opts != nil {
		s.opts = *opts
	}
	if s.opts.ToolListChanged != nil {
		s.toolsChanged = make(chan struct{}, 1)
	}

	streamable, isStreamable := transport.(*mcp.StreamableClientTransport)
	if isStreamable {
		copied := *streamable
		s.http = &streamableRoundTripper{}
		copied.HTTPClient = s.http.wrap(streamable.HTTPClient)
		streamable = &copied
		transport = streamable
	}

	conn, err := transport.Connect(ctx)
	if err != nil {
		s.cancel()
		return nil, err
	}
	s.conn = conn
	go s.read()

	result, err := s.handshake(ctx)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.initializeResult = result

	if s.opts.ToolListChanged != nil && announcesToolChanges(result) {
		s.callbackDone = make(chan struct{})
		go s.runCallbacks()
		switch {
		case isStateless(result.ProtocolVersion):
			s.changes = newSubscription(s)
		case isStreamable && !streamable.DisableStandaloneSSE:
			s.changes = newChangeStream(s, streamable, conn.SessionID(), result.ProtocolVersion)
		}
	}
	return s, nil
}

// InitializeResult is what the server said about itself: its answer to
// initialize, or for the stateless protocol, its answer to server/discover in
// the same form.
func (s *Session) InitializeResult() *mcp.InitializeResult {
	return s.initializeResult
}

// ID is the session ID the server assigned, if it assigned one.
func (s *Session) ID() string {
	return s.conn.SessionID()
}

// Tools returns an iterator over the tools the server offers. It reads page by
// page, from params.Cursor or from the first page when params is nil, and
// stops early when the caller does. When a page cannot be read, it yields the
// error once and stops.
//
// The InputSchema and OutputSchema of each tool are the json.RawMessage the
// server sent, or nil when it sent none.
func (s *Session) Tools(ctx context.Context, params *mcp.ListToolsParams) iter.Seq2[*mcp.Tool, error] {
	return func(yield func(*mcp.Tool, error) bool) {
		var request mcp.ListToolsParams
		if params != nil {
			request = *params
		}
		cursorsSeen := map[string]bool{}
		for {
			tools, nextCursor, err := s.listToolsPage(ctx, &request)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, tool := range tools {
				if !yield(tool, nil) {
					return
				}
			}
			if nextCursor == "" {
				return
			}
			// A server that hands out a cursor it already gave would be read
			// forever.
			if cursorsSeen[nextCursor] {
				yield(nil, fmt.Errorf("the server sent the tools/list cursor %q twice", nextCursor))
				return
			}
			cursorsSeen[nextCursor] = true
			request.Cursor = nextCursor
		}
	}
}

func (s *Session) listToolsPage(ctx context.Context, params *mcp.ListToolsParams) ([]*mcp.Tool, string, error) {
	var page struct {
		Tools      []json.RawMessage `json:"tools"`
		NextCursor string            `json:"nextCursor"`
	}
	if err := s.call(ctx, methodListTools, params, &page); err != nil {
		return nil, "", err
	}
	tools := make([]*mcp.Tool, 0, len(page.Tools))
	for _, raw := range page.Tools {
		tool, err := readTool(raw)
		if err != nil {
			return nil, "", err
		}
		tools = append(tools, tool)
	}
	return tools, page.NextCursor, nil
}

// CallTool calls a tool and waits for its result.
//
// The StructuredContent of the result is the json.RawMessage the server sent,
// or nil when it sent none.
func (s *Session) CallTool(ctx context.Context, params *mcp.CallToolParams) (*mcp.CallToolResult, error) {
	var raw json.RawMessage
	if err := s.call(ctx, methodCallTool, params, &raw); err != nil {
		return nil, err
	}
	return readCallToolResult(raw)
}

// Wait blocks until the connection ends. It returns nil when the session was
// closed with Close, and the reason otherwise.
func (s *Session) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close ends the session. It may be called more than once, and from a
// ToolListChanged callback. Close does not wait for a callback that is running
// when it is called, since that callback may be what called it.
func (s *Session) Close() error {
	s.mu.Lock()
	alreadyClosing := s.closing
	s.closing = true
	s.mu.Unlock()

	if s.changes != nil {
		s.changes.stop()
	}
	s.cancelCallsInFlight()
	s.cancel()
	err := s.conn.Close()
	<-s.done

	s.mu.Lock()
	s.tasksClosed = true
	s.mu.Unlock()
	s.tasks.Wait()

	if s.callbackDone != nil {
		s.callbackMu.Lock()
		running := s.inCallback
		s.callbackMu.Unlock()
		if !running {
			<-s.callbackDone
		}
	}

	if alreadyClosing {
		return nil
	}
	return err
}

// A pendingCall is a call waiting for its response.
type pendingCall struct {
	method   string
	response chan *jsonrpc.Response
}

// cancelCallsInFlight tells the server that every call still waiting for its
// response is cancelled, and waits up to notifyTimeout for that to be sent. A
// server goes on running a call it is not told about, and one that keeps a
// session open waits for such calls before the session can end.
func (s *Session) cancelCallsInFlight() {
	s.mu.Lock()
	var keys []string
	for key, call := range s.pending {
		if call.method != methodInitialize && call.method != methodDiscover {
			keys = append(keys, key)
		}
	}
	s.mu.Unlock()
	if len(keys) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(s.ctx, notifyTimeout)
	defer cancel()
	var sent sync.WaitGroup
	for _, key := range keys {
		sent.Go(func() {
			_ = s.notify(ctx, methodCancelled, &mcp.CancelledParams{RequestID: key, Reason: "the session is closing"})
		})
	}
	sent.Wait()
}

// readTool reads a tool with the SDK's type, keeping its schemas as the bytes
// the server sent. Read on its own, the SDK's type would hold a schema as a
// map[string]any, which loses the order of its keys. encoding/json decodes
// into a pointer an interface already holds, so the schemas land in the two
// json.RawMessage values instead. A schema the server sent as null, or did not
// send, leaves its value empty.
func readTool(raw json.RawMessage) (*mcp.Tool, error) {
	var inputSchema, outputSchema json.RawMessage
	tool := mcp.Tool{InputSchema: &inputSchema, OutputSchema: &outputSchema}
	if err := json.Unmarshal(raw, &tool); err != nil {
		return nil, fmt.Errorf("read a tool: %w", err)
	}
	tool.InputSchema = rawOrNil(inputSchema)
	tool.OutputSchema = rawOrNil(outputSchema)
	return &tool, nil
}

// readCallToolResult reads the result of a tool call with the SDK's type,
// keeping its structured content as the bytes the server sent. The SDK's type
// reads itself through a struct of its own, so the pointer readTool relies on
// would be dropped; the structured content is read a second time instead.
func readCallToolResult(raw json.RawMessage) (*mcp.CallToolResult, error) {
	var result mcp.CallToolResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("read the result of %s: %w", methodCallTool, err)
	}
	var structured struct {
		StructuredContent json.RawMessage `json:"structuredContent"`
	}
	if err := json.Unmarshal(raw, &structured); err != nil {
		return nil, fmt.Errorf("read the structured content of a tool result: %w", err)
	}
	result.StructuredContent = rawOrNil(structured.StructuredContent)
	return &result, nil
}

func rawOrNil(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return raw
}

func announcesToolChanges(result *mcp.InitializeResult) bool {
	return result.Capabilities != nil && result.Capabilities.Tools != nil && result.Capabilities.Tools.ListChanged
}

// read takes every message off the connection until it ends.
func (s *Session) read() {
	for {
		msg, err := s.conn.Read(s.ctx)
		if err != nil {
			s.finish(err)
			return
		}
		s.handle(msg)
	}
}

// handle acts on one message from the server, from the connection or from the
// stream of a streamable HTTP server.
func (s *Session) handle(msg jsonrpc.Message) {
	switch msg := msg.(type) {
	case *jsonrpc.Response:
		key, _ := msg.ID.Raw().(string)
		s.mu.Lock()
		waiting := s.pending[key]
		delete(s.pending, key)
		s.mu.Unlock()
		if waiting != nil {
			waiting.response <- msg
		}
	case *jsonrpc.Request:
		if msg.IsCall() {
			s.spawn(func() { s.answer(msg) })
			return
		}
		if msg.Method == methodToolListChanged && s.toolsChanged != nil {
			select {
			case s.toolsChanged <- struct{}{}:
			default:
			}
		}
	}
}

// runCallbacks reports changes to the tools, one call at a time, until the
// session ends.
func (s *Session) runCallbacks() {
	defer close(s.callbackDone)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.toolsChanged:
		}
		// Close checks inCallback under the same lock after ending ctx, so
		// once it has looked, no callback starts.
		s.callbackMu.Lock()
		if s.ctx.Err() != nil {
			s.callbackMu.Unlock()
			return
		}
		s.inCallback = true
		s.callbackMu.Unlock()

		s.opts.ToolListChanged()

		s.callbackMu.Lock()
		s.inCallback = false
		s.callbackMu.Unlock()
	}
}

// answer replies to a request from the server. It answers ping, and refuses
// everything else, such as sampling and elicitation, which this client does
// not offer.
func (s *Session) answer(req *jsonrpc.Request) {
	resp := &jsonrpc.Response{ID: req.ID}
	if req.Method == methodPing {
		resp.Result = json.RawMessage(`{}`)
	} else {
		resp.Error = &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "this client does not support " + req.Method}
	}
	ctx, cancel := context.WithTimeout(s.ctx, notifyTimeout)
	defer cancel()
	_ = s.conn.Write(ctx, resp)
}

// spawn runs task on a goroutine that Close waits for. Once Close has begun
// waiting, it runs nothing.
func (s *Session) spawn(task func()) {
	s.mu.Lock()
	if s.tasksClosed {
		s.mu.Unlock()
		return
	}
	s.tasks.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.tasks.Done()
		task()
	}()
}

// fail ends the session from this side, for a reason the connection cannot
// see. Wait returns err.
func (s *Session) fail(err error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
	s.conn.Close()
}

// finish records why the connection ended and wakes every call waiting on it.
func (s *Session) finish(readErr error) {
	s.mu.Lock()
	err := readErr
	if s.failure != nil {
		err = s.failure
	}
	if s.closing {
		err = nil
	}
	s.err = err
	s.pending = nil
	s.mu.Unlock()
	s.cancel()
	close(s.done)
}

// ended is the error a call gets once the connection has ended.
func (s *Session) ended() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		return errors.New("the session is closed")
	}
	return fmt.Errorf("the connection to the server ended: %w", s.err)
}

// call sends a request and waits for its response, which it reads into result.
// When ctx ends first, the server is told the request was cancelled.
func (s *Session) call(ctx context.Context, method string, params, result any) error {
	raw, err := s.encodeParams(params)
	if err != nil {
		return fmt.Errorf("write the params of %s: %w", method, err)
	}
	key := strconv.FormatInt(s.lastID.Add(1), 10)
	id, err := jsonrpc.MakeID(key)
	if err != nil {
		return err
	}

	response := make(chan *jsonrpc.Response, 1)
	s.mu.Lock()
	if s.pending == nil {
		s.mu.Unlock()
		return s.ended()
	}
	s.pending[key] = &pendingCall{method: method, response: response}
	s.mu.Unlock()

	// The request lives as long as the call, and no longer than the session.
	// Over streamable HTTP, its context is that of the HTTP request, which
	// carries the answer: sending waits for the server to start answering,
	// and the answer streams after that. Ending the request is also how a
	// server of the stateless protocol learns a call was abandoned.
	requestCtx, endRequest := context.WithCancel(ctx)
	defer endRequest()
	stopEndingWithSession := context.AfterFunc(s.ctx, endRequest)
	defer stopEndingWithSession()

	if err := s.conn.Write(requestCtx, &jsonrpc.Request{ID: id, Method: method, Params: raw}); err != nil {
		s.forget(key)
		// The server may already have the request.
		if ctx.Err() != nil {
			s.cancelRequest(method, key, ctx.Err())
			return ctx.Err()
		}
		if s.ctx.Err() != nil {
			<-s.done
			return s.ended()
		}
		return fmt.Errorf("send %s: %w", method, err)
	}

	select {
	case resp := <-response:
		if resp.Error != nil {
			return fmt.Errorf("%s: %w", method, resp.Error)
		}
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("read the result of %s: %w", method, err)
		}
		return nil
	case <-ctx.Done():
		s.forget(key)
		s.cancelRequest(method, key, ctx.Err())
		return ctx.Err()
	case <-s.done:
		return s.ended()
	}
}

// encodeParams writes the params of a request. Absent params are written as
// an empty object, since JSON-RPC does not allow null params. In the stateless
// protocol, the request metadata is added.
func (s *Session) encodeParams(params any) (json.RawMessage, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		raw = json.RawMessage(`{}`)
	}
	if isStateless(s.protocolVersion) {
		return s.withRequestMeta(raw)
	}
	return raw, nil
}

// cancelRequest tells the server that a request was cancelled. It does so on
// a goroutine of its own, so the caller whose context ended returns at once.
// The handshake is never cancelled, only abandoned.
func (s *Session) cancelRequest(method, key string, cause error) {
	if method == methodInitialize || method == methodDiscover {
		return
	}
	s.spawn(func() {
		ctx, cancel := context.WithTimeout(s.ctx, notifyTimeout)
		defer cancel()
		_ = s.notify(ctx, methodCancelled, &mcp.CancelledParams{RequestID: key, Reason: cause.Error()})
	})
}

func (s *Session) forget(key string) {
	s.mu.Lock()
	delete(s.pending, key)
	s.mu.Unlock()
}

func (s *Session) notify(ctx context.Context, method string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	return s.conn.Write(ctx, &jsonrpc.Request{Method: method, Params: raw})
}
