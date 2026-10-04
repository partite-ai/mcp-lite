package client

import (
	"encoding/json"
	"testing"
)

// A schema is kept as the bytes the server sent, and one the server sent as
// null or left out is nil.
func TestReadToolKeepsSchemaBytes(t *testing.T) {
	const input = `{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"number"}}}`
	cases := []struct {
		name       string
		raw        string
		wantInput  string
		wantOutput string
	}{
		{"both", `{"name":"t","inputSchema":` + input + `,"outputSchema":{"type":"object","properties":{"y":{},"b":{}}}}`, input, `{"type":"object","properties":{"y":{},"b":{}}}`},
		{"no output", `{"name":"t","inputSchema":` + input + `}`, input, ""},
		{"null output", `{"name":"t","inputSchema":` + input + `,"outputSchema":null}`, input, ""},
		{"no schemas", `{"name":"t"}`, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tool, err := readTool(json.RawMessage(c.raw))
			if err != nil {
				t.Fatal(err)
			}
			if tool.Name != "t" {
				t.Errorf("name is %q", tool.Name)
			}
			checkSchema(t, "input", tool.InputSchema, c.wantInput)
			checkSchema(t, "output", tool.OutputSchema, c.wantOutput)
		})
	}
}

func checkSchema(t *testing.T, which string, got any, want string) {
	t.Helper()
	if want == "" {
		if got != nil {
			t.Errorf("%s schema is %#v; want nil", which, got)
		}
		return
	}
	raw, ok := got.(json.RawMessage)
	if !ok {
		t.Fatalf("%s schema is %T; want json.RawMessage", which, got)
	}
	if string(raw) != want {
		t.Errorf("%s schema is %s; want %s", which, raw, want)
	}
}
