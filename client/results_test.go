package client

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The structured content of a result is kept as the bytes the server sent.
func TestStructuredContentKeepsItsOrder(t *testing.T) {
	const structured = `{"zeta":1,"alpha":{"y":true,"b":12345678901234567890}}`
	forEachSetup(t, func(t *testing.T, setup setup) {
		server := setup.server()
		server.AddTool(&mcp.Tool{Name: "structured", InputSchema: json.RawMessage(`{"type":"object"}`)},
			func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{
					Content:           []mcp.Content{&mcp.TextContent{Text: structured}},
					StructuredContent: json.RawMessage(structured),
				}, nil
			})
		s := setup.connect(t, server, nil)
		result, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "structured"})
		if err != nil {
			t.Fatal(err)
		}
		raw, ok := result.StructuredContent.(json.RawMessage)
		if !ok || string(raw) != structured {
			t.Errorf("structured content is %#v; want %s", result.StructuredContent, structured)
		}
		if len(result.Content) != 1 {
			t.Errorf("result has %d content blocks; want 1", len(result.Content))
		}
	})
}

// Tools reads no further than the caller does.
func TestToolsStopsWhenTheCallerDoes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer()
		closeServerSessions(t, server)
		handler := sdkHandler(server, &mcp.StreamableHTTPOptions{Stateless: true})
		var pages atomic.Int32
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && readRPC(r).Method == methodListTools {
				pages.Add(1)
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		for tool, err := range s.Tools(context.Background(), nil) {
			if err != nil {
				t.Fatal(err)
			}
			if tool.Name != "echo" {
				t.Errorf("first tool is %q; want echo", tool.Name)
			}
			break
		}
		if pages.Load() != 1 {
			t.Errorf("read %d pages; want 1", pages.Load())
		}
	})
}

// A server that hands out the same cursor again ends the listing with an
// error rather than being read forever.
func TestToolsStopsOnARepeatedCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := newServer(handshakeVersions...)
		closeServerSessions(t, server)
		handler := sdkHandler(server, nil)
		s := connect(t, serveHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				if req := readRPC(r); req.Method == methodListTools {
					writeRPCResult(w, req.ID, `{"tools":[{"name":"t","inputSchema":{"type":"object"}}],"nextCursor":"same"}`)
					return
				}
			}
			handler.ServeHTTP(w, r)
		})).transport(), nil)

		var tools int
		var listErr error
		for _, err := range s.Tools(context.Background(), nil) {
			if err != nil {
				listErr = err
				break
			}
			tools++
		}
		if listErr == nil || tools != 2 {
			t.Errorf("listed %d tools then %v; want 2 tools then an error", tools, listErr)
		}
	})
}
