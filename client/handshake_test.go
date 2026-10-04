package client

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A server that cannot answer discover for now, such as one that answers 503,
// fails the connection rather than settling on an older protocol.
func TestATransientFailureOnDiscoverIsNotAFallback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		var initialized atomic.Bool
		srv := serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				switch readRPC(r).Method {
				case methodDiscover:
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				case methodInitialize:
					initialized.Store(true)
				}
			}
			handler.ServeHTTP(w, r)
		}))

		if _, err := Connect(context.Background(), srv.transport(), nil); err == nil {
			t.Fatal("Connect succeeded; want the failure to answer discover")
		}
		if initialized.Load() {
			t.Error("the client fell back to initialize after a 503")
		}
	})
}

// A server that never answers discover gets the initialize handshake once the
// wait for discover is over.
func TestAnUnansweredDiscoverFallsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		started := time.Now()
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && readRPC(r).Method == methodDiscover {
				<-r.Context().Done()
				return
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		if got := s.InitializeResult().ProtocolVersion; got != newestHandshake {
			t.Errorf("negotiated %q; want %q", got, newestHandshake)
		}
		if elapsed := time.Since(started); elapsed != discoverTimeout {
			t.Errorf("connecting took %v; want the wait for discover, %v", elapsed, discoverTimeout)
		}
	})
}

// A server that answers discover with no stateless version this client speaks
// gets the initialize handshake.
func TestDiscoverWithoutASharedVersionFallsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				if req := readRPC(r); req.Method == methodDiscover {
					writeRPCResult(w, req.ID, `{"supportedVersions":["2099-01-01"],"capabilities":{}}`)
					return
				}
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		if got := s.InitializeResult().ProtocolVersion; got != newestHandshake {
			t.Errorf("negotiated %q; want %q", got, newestHandshake)
		}
	})
}

func TestAnUnknownVersionFromInitializeFailsTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv := serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch req := readRPC(r); req.Method {
			case methodDiscover:
				writeRPCError(w, req.ID, -32601, "method not found")
			case methodInitialize:
				writeRPCResult(w, req.ID, `{"protocolVersion":"1999-01-01","capabilities":{},"serverInfo":{"name":"old","version":"1"}}`)
			default:
				w.WriteHeader(http.StatusAccepted)
			}
		}))

		_, err := Connect(context.Background(), srv.transport(), nil)
		if err == nil || !strings.Contains(err.Error(), "1999-01-01") {
			t.Fatalf("Connect returned %v; want an error naming the version", err)
		}
	})
}

func TestRequestMeta(t *testing.T) {
	s := &Session{protocolVersion: newestStateless}

	t.Run("absent params", func(t *testing.T) {
		raw, err := s.encodeParams(nil)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields["_meta"] == nil {
			t.Errorf("absent params were written as %s; want an object with _meta", raw)
		}
	})

	t.Run("the caller's meta is kept exactly", func(t *testing.T) {
		raw, err := s.encodeParams(&mcp.CallToolParams{Name: "t", Meta: mcp.Meta{
			"progressToken":            json.RawMessage(`12345678901234567890`),
			"nested":                   json.RawMessage(`{"z":1,"a":2}`),
			mcp.MetaKeyProtocolVersion: "1999-01-01",
		}})
		if err != nil {
			t.Fatal(err)
		}
		var params struct {
			Meta map[string]json.RawMessage `json:"_meta"`
		}
		if err := json.Unmarshal(raw, &params); err != nil {
			t.Fatal(err)
		}
		if got := string(params.Meta["progressToken"]); got != `12345678901234567890` {
			t.Errorf("progress token is %s; want it exact", got)
		}
		if got := string(params.Meta["nested"]); got != `{"z":1,"a":2}` {
			t.Errorf("nested meta is %s; want its order kept", got)
		}
		if got := string(params.Meta[mcp.MetaKeyProtocolVersion]); got != `"`+newestStateless+`"` {
			t.Errorf("protocol version is %s; want the session's", got)
		}
	})

	t.Run("malformed meta", func(t *testing.T) {
		if _, err := s.withRequestMeta(json.RawMessage(`{"_meta":"not an object"}`)); err == nil {
			t.Error("a _meta that is not an object was accepted")
		}
	})

	t.Run("handshake protocol", func(t *testing.T) {
		old := &Session{protocolVersion: newestHandshake}
		raw, err := old.encodeParams(nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != `{}` {
			t.Errorf("absent params were written as %s; want {}", raw)
		}
	})
}
