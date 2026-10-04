package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// discoverTimeout bounds how long the client waits for an answer to
// server/discover before trying the initialize handshake. A server from before
// the stateless protocol may drop a request it does not know rather than
// answer it.
const discoverTimeout = 10 * time.Second

// handshake agrees on a protocol version with the server and learns what it
// offers. It tries the stateless versions first, with server/discover, and
// falls back to the initialize handshake when the server refuses them.
func (s *Session) handshake(ctx context.Context) (*mcp.InitializeResult, error) {
	discoverCtx, cancel := context.WithTimeout(ctx, discoverTimeout)
	result, err := s.discover(discoverCtx)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		select {
		case <-s.done:
			return nil, err
		default:
		}
		if !s.refusedDiscover(err) {
			return nil, fmt.Errorf("ask the server what it supports: %w", err)
		}
		if result, err = s.initialize(ctx); err != nil {
			return nil, err
		}
	}
	s.protocolVersion = result.ProtocolVersion
	s.http.setProtocolVersion(result.ProtocolVersion)
	return result, nil
}

// refusedDiscover reports whether the server answered discover in a way that
// means it speaks no stateless version, rather than failing to answer at all.
// A failure to answer, such as a network error or a 503, ends the handshake:
// falling back would settle on an older protocol because of a passing fault.
func (s *Session) refusedDiscover(err error) bool {
	switch {
	case errors.Is(err, errNoStatelessVersion), serverRefused(err):
		return true
	case errors.Is(err, context.DeadlineExceeded):
		// The server took longer than discoverTimeout, while the caller's
		// context is still alive.
		return true
	}
	// Over HTTP, a server may refuse with a plain HTTP error rather than a
	// JSON-RPC one.
	return s.http.lastPostRefused()
}

// errNoStatelessVersion says the server answered discover without offering a
// stateless version this client speaks.
var errNoStatelessVersion = errors.New("the server offers no stateless protocol version this client speaks")

// discover asks the server with server/discover what it supports, the way a
// stateless session starts. It asks for the newest stateless version, and once
// more for an older one if the server names one it would take instead. While
// this client speaks only one stateless version, the second ask never happens.
func (s *Session) discover(ctx context.Context) (*mcp.InitializeResult, error) {
	version := statelessVersions[0]
	for attempt := 0; ; attempt++ {
		var result mcp.DiscoverResult
		err := s.call(ctx, methodDiscover, &mcp.DiscoverParams{Meta: s.requestMeta(version)}, &result)
		if err == nil {
			negotiated := newestShared(statelessVersions, result.SupportedVersions)
			if negotiated == "" {
				return nil, errNoStatelessVersion
			}
			return discoverResult(negotiated, &result), nil
		}
		other := unsupportedVersionOffer(err)
		if attempt > 0 || other == "" || other == version {
			return nil, err
		}
		version = other
	}
}

// unsupportedVersionOffer reads a refusal of the version asked for, and
// returns the newest stateless version the server said it supports instead.
func unsupportedVersionOffer(err error) string {
	var refusal *jsonrpc.Error
	if !errors.As(err, &refusal) || refusal.Code != mcp.CodeUnsupportedProtocolVersion || len(refusal.Data) == 0 {
		return ""
	}
	var data mcp.UnsupportedProtocolVersionData
	if json.Unmarshal(refusal.Data, &data) != nil {
		return ""
	}
	return newestShared(statelessVersions, data.Supported)
}

// discoverResult puts the answer to discover in the form of the answer to
// initialize, so a caller reads both the same way.
func discoverResult(version string, result *mcp.DiscoverResult) *mcp.InitializeResult {
	var serverInfo *mcp.Implementation
	if raw, ok := result.Meta[mcp.MetaKeyServerInfo]; ok {
		// The value was read into the any of a map, so it is read again into its
		// type.
		if encoded, err := json.Marshal(raw); err == nil {
			serverInfo = new(mcp.Implementation)
			if json.Unmarshal(encoded, serverInfo) != nil {
				serverInfo = nil
			}
		}
	}
	return &mcp.InitializeResult{
		ProtocolVersion: version,
		Capabilities:    result.Capabilities,
		Instructions:    result.Instructions,
		ServerInfo:      serverInfo,
	}
}

// initialize makes the handshake of the versions before the stateless
// protocol.
func (s *Session) initialize(ctx context.Context) (*mcp.InitializeResult, error) {
	if len(handshakeVersions) == 0 {
		return nil, errors.New("this client speaks no protocol version with the initialize handshake")
	}
	var result mcp.InitializeResult
	err := s.call(ctx, methodInitialize, &mcp.InitializeParams{
		ProtocolVersion: handshakeVersions[0],
		ClientInfo:      s.clientInfo(),
		Capabilities:    &mcp.ClientCapabilities{},
	}, &result)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(handshakeVersions, result.ProtocolVersion) {
		return nil, fmt.Errorf("the server chose protocol version %q, which this client does not speak", result.ProtocolVersion)
	}
	// The version header goes on the notification that ends the handshake.
	s.http.setProtocolVersion(result.ProtocolVersion)
	if err := s.notify(ctx, methodInitialized, &mcp.InitializedParams{}); err != nil {
		return nil, fmt.Errorf("send %s: %w", methodInitialized, err)
	}
	return &result, nil
}

func (s *Session) clientInfo() *mcp.Implementation {
	if s.opts.ClientInfo != nil {
		return s.opts.ClientInfo
	}
	return &mcp.Implementation{Name: "mcp-lite"}
}

// requestMeta is what every request of the stateless protocol says about who
// is asking. This client offers no capabilities: it answers nothing the server
// could ask of it.
func (s *Session) requestMeta(version string) mcp.Meta {
	return mcp.Meta{
		mcp.MetaKeyProtocolVersion:    version,
		mcp.MetaKeyClientInfo:         s.clientInfo(),
		mcp.MetaKeyClientCapabilities: map[string]any{},
	}
}

// withRequestMeta sets the request metadata in the _meta of encoded params.
// Other keys of the caller's _meta are kept as the caller wrote them. The
// protocol's own keys are always this session's, since a request in another
// version than the session's is not one the server can read.
func (s *Session) withRequestMeta(params json.RawMessage) (json.RawMessage, error) {
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(params, &fields); err != nil {
		return nil, err
	}
	meta := map[string]json.RawMessage{}
	if raw, ok := fields["_meta"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, fmt.Errorf("read _meta: %w", err)
		}
	}
	for key, value := range s.requestMeta(s.protocolVersion) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		meta[key] = encoded
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	fields["_meta"] = encoded
	return json.Marshal(fields)
}

// codeRejectedByTransport is the code of the SDK's jsonrpc2.ErrRejected. The
// SDK's streamable HTTP transport wraps it around a failure to reach the
// server, such as a network error or a 503, so it is a JSON-RPC error in form
// only. The SDK does not export it.
const codeRejectedByTransport = -32005

// serverRefused reports whether err is the server's JSON-RPC answer to a
// request, rather than a failure to reach the server.
func serverRefused(err error) bool {
	var answer *jsonrpc.Error
	return errors.As(err, &answer) && answer.Code != codeRejectedByTransport
}
