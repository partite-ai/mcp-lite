package client

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestTheProtocolVersionIsNegotiated(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, setup.server(), nil)
		result := s.InitializeResult()
		if result.ProtocolVersion != setup.wantVersion {
			t.Errorf("negotiated %q; want %q", result.ProtocolVersion, setup.wantVersion)
		}
		if result.ServerInfo == nil || result.ServerInfo.Name != "test" {
			t.Errorf("server info is %+v", result.ServerInfo)
		}
		if result.Capabilities == nil || result.Capabilities.Tools == nil {
			t.Errorf("capabilities are %+v", result.Capabilities)
		}
	})
}

func TestListToolsKeepsTheSchemaAsSent(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, setup.server(), nil)
		tools, err := allTools(s)
		if err != nil {
			t.Fatal(err)
		}
		if len(tools) != 2 || tools[0].Name != "echo" || tools[1].Name != "plain" {
			t.Fatalf("listed %d tools: %+v", len(tools), tools)
		}
		schema, ok := tools[0].InputSchema.(json.RawMessage)
		if !ok {
			t.Fatalf("input schema is %T; want json.RawMessage", tools[0].InputSchema)
		}
		if string(schema) != orderedSchema {
			t.Errorf("input schema is\n%s\nwant\n%s", schema, orderedSchema)
		}
		if tools[0].OutputSchema != nil {
			t.Errorf("output schema is %v; want nil", tools[0].OutputSchema)
		}
		if tools[0].Description != "Echo the arguments." {
			t.Errorf("description is %q", tools[0].Description)
		}

		// The tool marshals back with the schema as it came.
		out, err := json.Marshal(tools[0])
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(out, []byte(orderedSchema)) {
			t.Errorf("marshalled tool %s does not hold the schema as sent", out)
		}
	})
}

func TestCallTool(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, setup.server(), nil)
		result, err := s.CallTool(context.Background(), &mcp.CallToolParams{
			Name:      "echo",
			Arguments: json.RawMessage(`{"zeta":"z","alpha":1}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Content) != 1 {
			t.Fatalf("result has %d content blocks", len(result.Content))
		}
		text, ok := result.Content[0].(*mcp.TextContent)
		if !ok || text.Text != `{"zeta":"z","alpha":1}` {
			t.Errorf("result content is %#v", result.Content[0])
		}
	})
}

func TestAnErrorFromTheServerIsReturned(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, setup.server(), nil)
		if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "missing"}); err == nil {
			t.Fatal("calling a missing tool returned no error")
		}
	})
}

func TestToolListChangedIsHeard(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		server := setup.server()
		changed := make(chan struct{}, 1)
		setup.connect(t, server, &Options{ToolListChanged: func() {
			select {
			case changed <- struct{}{}:
			default:
			}
		}})
		// Let the subscription or the change stream open.
		synctest.Wait()

		addTool(server, 0)
		select {
		case <-changed:
		case <-time.After(time.Minute):
			t.Fatal("no change to the tool list was heard")
		}
	})
}

func TestAPingFromTheServerIsAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		closeServerSessions(t, server)
		clientTransport, serverTransport := mcp.NewInMemoryTransports()
		serverSession, err := server.Connect(context.Background(), serverTransport, nil)
		if err != nil {
			t.Fatal(err)
		}
		connect(t, clientTransport, nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := serverSession.Ping(ctx, nil); err != nil {
			t.Fatalf("ping: %v", err)
		}
	})
}

// After the handshake, every HTTP request carries the agreed protocol version.
func TestRequestsCarryTheProtocolVersion(t *testing.T) {
	cases := []struct {
		name        string
		handlerOpts *mcp.StreamableHTTPOptions
		want        string
		methods     []string
	}{
		{"stateless", &mcp.StreamableHTTPOptions{Stateless: true}, newestStateless, []string{methodDiscover, methodListTools}},
		{"handshake", nil, newestHandshake, []string{methodInitialized, methodListTools}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var mu sync.Mutex
				versions := map[string]string{}
				server := newServer()
				closeServerSessions(t, server)
				handler := sdkHandler(server, c.handlerOpts)
				s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost {
						method := readRPC(r).Method
						mu.Lock()
						versions[method] = r.Header.Get(headerProtocolVersion)
						mu.Unlock()
					}
					handler.ServeHTTP(w, r)
				})).transport(), nil)
				if _, err := allTools(s); err != nil {
					t.Fatal(err)
				}

				mu.Lock()
				defer mu.Unlock()
				for _, method := range c.methods {
					if versions[method] != c.want {
						t.Errorf("%s went with protocol version %q; want %q", method, versions[method], c.want)
					}
				}
			})
		})
	}
}

// A server from before the stateless protocol may refuse discover with a plain
// HTTP error rather than a JSON-RPC one. The client falls back all the same.
func TestDiscoverRefusedOverHTTPFallsBack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && readRPC(r).Method == methodDiscover {
				http.Error(w, "Bad Request: missing session ID", http.StatusBadRequest)
				return
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		if got := s.InitializeResult().ProtocolVersion; got != newestHandshake {
			t.Errorf("negotiated %q; want %q", got, newestHandshake)
		}
		if _, err := allTools(s); err != nil {
			t.Fatal(err)
		}
	})
}

// Metadata the caller sets on a call is kept beside the request metadata.
func TestCallerMetaIsKept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		seen := make(chan mcp.Meta, 1)
		server.AddTool(&mcp.Tool{Name: "meta", InputSchema: json.RawMessage(`{"type":"object"}`)},
			func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				seen <- req.Params.Meta
				return &mcp.CallToolResult{}, nil
			})
		s := inMemory(t, server, nil)
		if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "meta", Meta: mcp.Meta{"trace": "abc"}}); err != nil {
			t.Fatal(err)
		}
		meta := <-seen
		if meta["trace"] != "abc" {
			t.Errorf("the server saw _meta %v; want the caller's trace kept", meta)
		}
		if meta[mcp.MetaKeyProtocolVersion] != newestStateless {
			t.Errorf("the server saw _meta %v; want the protocol version", meta)
		}
	})
}

func TestACallAfterCloseFails(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, setup.server(), nil)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Wait(); err != nil {
			t.Errorf("Wait after Close returned %v; want nil", err)
		}
		if _, err := allTools(s); err == nil {
			t.Error("listing tools after Close returned no error")
		}
	})
}
