package client

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// withPeople adds a resource template that reads people, as a server that
// links them would.
func withPeople(server *mcp.Server) *mcp.Server {
	server.AddResourceTemplate(&mcp.ResourceTemplate{
		URITemplate: "person://{id}",
		Name:        "person",
		MIMEType:    "text/vnd.example.person",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		if req.Params.URI != "person://ada" {
			return nil, mcp.ResourceNotFoundError(req.Params.URI)
		}
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
			URI: req.Params.URI, MIMEType: "text/vnd.example.person", Text: "# Ada",
		}}}, nil
	})
	return server
}

func TestReadResource(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, withPeople(setup.server()), nil)
		result, err := s.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: "person://ada"})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Contents) != 1 || result.Contents[0].Text != "# Ada" || result.Contents[0].MIMEType != "text/vnd.example.person" {
			t.Errorf("read %+v", result.Contents)
		}
	})
}

// A resource that cannot be read answers with an error.
func TestReadingAMissingResourceFails(t *testing.T) {
	forEachSetup(t, func(t *testing.T, setup setup) {
		s := setup.connect(t, withPeople(setup.server()), nil)
		if _, err := s.ReadResource(context.Background(), &mcp.ReadResourceParams{URI: "person://nobody"}); err == nil {
			t.Error("reading a missing resource returned no error")
		}
	})
}
