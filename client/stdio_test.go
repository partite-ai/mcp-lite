package client

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serveStdioEnv makes the test binary serve newServer over stdio instead of
// running tests, so a test can start it as an MCP server.
const serveStdioEnv = "MCP_LITE_TEST_SERVE_STDIO"

func TestMain(m *testing.M) {
	if os.Getenv(serveStdioEnv) != "" {
		if err := newServer().Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// The client works over stdio, with the server as a child process.
func TestStdio(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), serveStdioEnv+"=1")
	s, err := Connect(context.Background(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if got := s.InitializeResult().ProtocolVersion; got != newestStateless {
		t.Errorf("negotiated %q; want %q", got, newestStateless)
	}
	tools, err := allTools(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(tools) != 2 {
		t.Fatalf("listed %d tools; want 2", len(tools))
	}
	if schema, _ := tools[0].InputSchema.(json.RawMessage); string(schema) != orderedSchema {
		t.Errorf("input schema is %s; want %s", schema, orderedSchema)
	}
	result, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "echo", Arguments: json.RawMessage(`{"zeta":"z","alpha":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if text, ok := result.Content[0].(*mcp.TextContent); !ok || text.Text != `{"zeta":"z","alpha":1}` {
		t.Errorf("result content is %#v", result.Content[0])
	}
}
