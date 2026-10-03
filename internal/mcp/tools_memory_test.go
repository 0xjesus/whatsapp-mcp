package mcp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type memoryRoundTrip func(*http.Request) (*http.Response, error)

func (f memoryRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func memoryCall(t *testing.T, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	tool := NewServer(nil, nil).MCP().ListTools()[name]
	if tool == nil {
		t.Fatalf("missing memory tool %s", name)
	}
	res, err := tool.Handler(context.Background(), mcp.CallToolRequest{Params: mcp.CallToolParams{Arguments: args}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestMemoryToolsProxyCorrectSourceAndRoutes(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	for _, tc := range []struct {
		name, path, method string
		args               map[string]any
	}{
		{"semantic_search", "/search", "POST", map[string]any{"query": "synthetic", "after": "2026-01-01", "limit": float64(4)}},
		{"index_status", "/status", "GET", map[string]any{}},
		{"history_analytics", "/analytics", "POST", map[string]any{"group_by": "month", "before": float64(1789398000)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			http.DefaultTransport = memoryRoundTrip(func(r *http.Request) (*http.Response, error) {
				called = true
				if r.URL.Host != "127.0.0.1:7256" || r.URL.Path != tc.path || r.Method != tc.method {
					t.Errorf("unexpected route %s %s", r.Method, r.URL)
				}
				if deadline, ok := r.Context().Deadline(); !ok || time.Until(deadline) > 46*time.Second {
					t.Error("missing bounded timeout")
				}
				if r.Method == "GET" {
					if r.URL.Query().Get("source") != "whatsapp" {
						t.Error("missing fixed source")
					}
				} else {
					var payload map[string]any
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Fatal(err)
					}
					if payload["source"] != "whatsapp" {
						t.Errorf("source=%v", payload["source"])
					}
					for key, value := range tc.args {
						if payload[key] != value {
							t.Errorf("lost argument %s", key)
						}
					}
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"hits":[{"message_id":"synthetic-id"}]}`))}, nil
			})
			result := memoryCall(t, tc.name, tc.args)
			if !called || result.IsError {
				t.Fatalf("proxy failed: called=%v, result=%+v", called, result)
			}
			content, _ := mcp.AsTextContent(result.Content[0])
			var body map[string]any
			if err := json.Unmarshal([]byte(content.Text), &body); err != nil || body["ok"] != true {
				t.Fatalf("response not preserved: %s", content.Text)
			}
		})
	}
}

func TestMemoryToolsRejectSourceOverrideAndBadArguments(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	http.DefaultTransport = memoryRoundTrip(func(r *http.Request) (*http.Response, error) { t.Fatal("invalid input reached HTTP"); return nil, nil })
	for _, args := range []map[string]any{
		{"query": "synthetic", "source": "telegram"}, {"query": "synthetic", "limit": 51},
		{"query": "synthetic", "limit": 1.5}, {"query": "synthetic", "mode": "sql"},
		{"query": " "}, {"query": "synthetic", "after": true},
	} {
		if !memoryCall(t, "semantic_search", args).IsError {
			t.Errorf("accepted invalid arguments %v", args)
		}
	}
	if !memoryCall(t, "index_status", map[string]any{"source": "telegram"}).IsError {
		t.Error("status accepted source override")
	}
	if !memoryCall(t, "history_analytics", map[string]any{"group_by": "sql"}).IsError {
		t.Error("analytics accepted invalid grouping")
	}
}

func TestMemoryToolsUnavailableDoesNotExposeBackendBody(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	http.DefaultTransport = memoryRoundTrip(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("private backend detail"))}, nil
	})
	result := memoryCall(t, "index_status", map[string]any{})
	content, _ := mcp.AsTextContent(result.Content[0])
	if !result.IsError || !strings.Contains(strings.ToLower(content.Text), "unavailable") || strings.Contains(content.Text, "private") {
		t.Fatalf("unsafe error: %s", content.Text)
	}
}

func TestMemorySearchPlanForwardedWithFixedSource(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	for _, plan := range []map[string]any{
		{}, {"keywords": []any{}}, {"person": "Synthetic Person"},
		{"keywords": []any{"invoice", "receipt"}, "semantic_queries": []any{"paid the invoice"}, "person": "Synthetic Person", "context": "event", "context_terms": []any{"payment"}},
		{"keywords": memoryTestStrings(8, strings.Repeat("ñ", 80)), "semantic_queries": memoryTestStrings(2, strings.Repeat("🧪", 150)), "person": strings.Repeat("ñ", 100), "context": "none", "context_terms": memoryTestStrings(6, strings.Repeat("ñ", 80))},
		{"context": "neighbors"},
	} {
		args := map[string]any{"query": "synthetic question", "plan": plan, "mode": "keyword", "chat": "synthetic chat", "sender": "synthetic sender", "after": "2026-01-01", "before": float64(1789398000), "limit": float64(3)}
		called := false
		http.DefaultTransport = memoryRoundTrip(func(r *http.Request) (*http.Response, error) {
			called = true
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["source"] != "whatsapp" {
				t.Errorf("missing fixed source: %v", payload["source"])
			}
			for key, value := range args {
				if !reflect.DeepEqual(payload[key], value) {
					t.Errorf("lost argument %s: got %v, want %v", key, payload[key], value)
				}
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
		})
		if result := memoryCall(t, "semantic_search", args); result.IsError || !called {
			t.Fatalf("valid plan rejected: plan=%v, result=%+v, called=%v", plan, result, called)
		}
	}
}

func memoryTestStrings(count int, value string) []any {
	values := make([]any, count)
	for i := range values {
		values[i] = value
	}
	return values
}

func TestMemorySearchPlanRejectsInvalidFieldsBeforeHTTP(t *testing.T) {
	previous := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = previous })
	http.DefaultTransport = memoryRoundTrip(func(r *http.Request) (*http.Response, error) {
		t.Error("invalid plan reached HTTP")
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	plans := []any{nil, []any{}, "keywords", 1, true,
		map[string]any{"unknown": "x"}, map[string]any{"source": "telegram"},
		map[string]any{"person": ""}, map[string]any{"person": "  "}, map[string]any{"person": 3},
		map[string]any{"person": map[string]any{}}, map[string]any{"person": strings.Repeat("ñ", 101)},
		map[string]any{"context": nil}, map[string]any{"context": []any{}}, map[string]any{"context": map[string]any{}},
		map[string]any{"context": "sql"}, map[string]any{"context": map[string]any{"unknown": "x"}},
	}
	for _, field := range []struct {
		name          string
		count, length int
	}{{"keywords", 8, 80}, {"semantic_queries", 2, 600}, {"context_terms", 6, 80}} {
		for _, value := range []any{nil, "text", map[string]any{}, true, memoryTestStrings(field.count+1, "x"),
			[]any{""}, []any{"  "}, []any{1}, []any{false}, []any{nil}, []any{[]any{"x"}}, []any{map[string]any{"unknown": "x"}}, []any{strings.Repeat("x", field.length+1)}} {
			plans = append(plans, map[string]any{field.name: value})
		}
	}
	plans = append(plans, map[string]any{"semantic_queries": []any{strings.Repeat("🧪", 150) + "a"}}, map[string]any{"semantic_queries": []any{strings.Repeat("ñ", 301)}}, map[string]any{"semantic_queries": []any{string([]byte{0xff})}})
	for i, plan := range plans {
		if !memoryCall(t, "semantic_search", map[string]any{"query": "synthetic", "plan": plan}).IsError {
			t.Errorf("accepted invalid plan %d: %v", i, plan)
		}
	}
	for _, name := range []string{"index_status", "history_analytics"} {
		if !memoryCall(t, name, map[string]any{"plan": map[string]any{}}).IsError {
			t.Errorf("%s accepted plan", name)
		}
	}
	if !memoryCall(t, "semantic_search", map[string]any{"plan": map[string]any{"keywords": []any{"synthetic"}}}).IsError {
		t.Error("plan bypassed required query")
	}
}

func TestMemorySearchPlanSchemaIsBoundedAndOptional(t *testing.T) {
	tools := NewServer(nil, nil).MCP().ListTools()
	raw, err := json.Marshal(tools["semantic_search"].Tool)
	if err != nil {
		t.Fatal(err)
	}
	var tool struct {
		InputSchema map[string]any `json:"inputSchema"`
	}
	if err := json.Unmarshal(raw, &tool); err != nil {
		t.Fatal(err)
	}
	schema := tool.InputSchema
	if !reflect.DeepEqual(schema["required"], []any{"query"}) || schema["additionalProperties"] != false {
		t.Fatalf("search schema changed required query/unknown-field contract: %s", raw)
	}
	plan, ok := schema["properties"].(map[string]any)["plan"].(map[string]any)
	if !ok {
		t.Fatal("missing plan object schema")
	}
	if plan["type"] != "object" || plan["additionalProperties"] != false || plan["required"] != nil {
		t.Fatalf("plan must be optional and closed: %v", plan)
	}
	properties := plan["properties"].(map[string]any)
	if len(properties) != 5 {
		t.Fatalf("unexpected plan properties: %v", properties)
	}
	for _, field := range []struct {
		name          string
		count, length float64
	}{{"keywords", 8, 80}, {"semantic_queries", 2, 600}, {"context_terms", 6, 80}} {
		array := properties[field.name].(map[string]any)
		items := array["items"].(map[string]any)
		if array["type"] != "array" || array["maxItems"] != field.count || items["type"] != "string" || items["minLength"] != float64(1) || items["maxLength"] != field.length {
			t.Errorf("wrong %s bounds: %v", field.name, array)
		}
	}
	if properties["person"].(map[string]any)["maxLength"] != float64(100) {
		t.Error("person must be bounded to 100 characters")
	}
	context := properties["context"].(map[string]any)
	if !reflect.DeepEqual(context["enum"], []any{"none", "neighbors", "event"}) || context["default"] != "neighbors" {
		t.Errorf("wrong context contract: %v", context)
	}
	for _, name := range []string{"index_status", "history_analytics"} {
		raw, err := json.Marshal(tools[name].Tool)
		if err != nil {
			t.Fatal(err)
		}
		tool.InputSchema = nil
		if err := json.Unmarshal(raw, &tool); err != nil {
			t.Fatal(err)
		}
		if _, ok := tool.InputSchema["properties"].(map[string]any)["plan"]; ok {
			t.Errorf("%s advertises plan", name)
		}
	}
}
