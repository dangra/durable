package durableotel_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/dangra/durable"
	"github.com/dangra/durable/contrib/durableotel"
	"github.com/dangra/durable/durabletest"
)

// logLines decodes one JSON object per line from buf.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestLogHandlerStampsSpanContext(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(durableotel.NewLogHandler(slog.NewJSONHandler(&buf, nil)))

	tp := sdktrace.NewTracerProvider() // AlwaysSample default
	defer tp.Shutdown(t.Context())
	ctx, span := tp.Tracer("test").Start(t.Context(), "op")

	logger.InfoContext(ctx, "in span", "k", "v")
	logger.InfoContext(t.Context(), "no span")
	logger.Info("no ctx at all")
	span.End()

	lines := logLines(t, &buf)
	if len(lines) != 3 {
		t.Fatalf("lines = %d, want 3", len(lines))
	}
	in, noSpan, noCtx := lines[0], lines[1], lines[2]
	if in["trace_id"] != span.SpanContext().TraceID().String() ||
		in["span_id"] != span.SpanContext().SpanID().String() {
		t.Fatalf("correlated line = %v, want trace/span of the active span", in)
	}
	if in["trace_flags"] != "01" {
		t.Fatalf("trace_flags = %v, want 01 for a sampled span", in["trace_flags"])
	}
	if in["k"] != "v" {
		t.Fatalf("record attrs lost: %v", in)
	}
	for name, line := range map[string]map[string]any{"noSpan": noSpan, "noCtx": noCtx} {
		if _, ok := line["trace_id"]; ok {
			t.Fatalf("%s line gained a trace_id: %v", name, line)
		}
	}
}

func TestLogHandlerForwardsAttrsGroupsAndLevel(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	logger := slog.New(durableotel.NewLogHandler(base)).With("app", "x").WithGroup("g")

	if logger.Enabled(t.Context(), slog.LevelInfo) {
		t.Fatal("Enabled(Info) = true, want the inner handler's Warn threshold")
	}
	logger.Warn("grouped", "k", "v")

	lines := logLines(t, &buf)
	if len(lines) != 1 || lines[0]["app"] != "x" {
		t.Fatalf("WithAttrs not forwarded: %v", lines)
	}
	g, ok := lines[0]["g"].(map[string]any)
	if !ok || g["k"] != "v" {
		t.Fatalf("WithGroup not forwarded: %v", lines[0])
	}
}

// TestLogCorrelationEndToEnd is the full DX story: a handler logging
// through inv.Logger().InfoContext(ctx, ...) produces a line whose
// trace_id/span_id match the attempt span the middleware started for
// that exact attempt.
func TestLogCorrelationEndToEnd(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer tp.Shutdown(t.Context())

	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	// The JSON handler guards its writer; the mutex is for reading buf
	// after the run.
	logger := slog.New(durableotel.NewLogHandler(slog.NewJSONHandler(&buf, nil)))

	def := durable.NewDefinition(durable.DefinitionConfig{
		ID: "logged",
		Steps: []durable.StepConfig{{
			ID: "work/v1",
			Run: func(ctx context.Context, inv *durable.Invocation) (proto.Message, error) {
				mu.Lock()
				defer mu.Unlock()
				inv.Logger().InfoContext(ctx, "working")
				return nil, nil
			},
		}},
	})
	engine := durable.NewEngine(durabletest.NewMemStore(), fastRetry,
		durable.WithLogger(logger),
		durable.WithMiddleware(durableotel.Middleware(durableotel.WithTracerProvider(tp))))
	pipe, err := def.Bind(engine)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := engine.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	run, _, err := pipe.Schedule(t.Context(), "res-1", nil)
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if _, err := run.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	engine.Stop(t.Context())

	var attempt trace.SpanContext
	for _, sp := range recorder.Ended() {
		if sp.Name() == "work/v1 forward" {
			attempt = sp.SpanContext()
		}
	}
	if !attempt.IsValid() {
		t.Fatal("attempt span not recorded")
	}

	mu.Lock()
	lines := logLines(t, &buf)
	mu.Unlock()
	for _, line := range lines {
		if line["msg"] != "working" {
			continue
		}
		if line["trace_id"] != attempt.TraceID().String() || line["span_id"] != attempt.SpanID().String() {
			t.Fatalf("handler line %v does not join attempt span %s/%s",
				line, attempt.TraceID(), attempt.SpanID())
		}
		if line["run"] == nil || line["step"] != "work/v1" {
			t.Fatalf("canonical durable keys missing from %v", line)
		}
		return
	}
	t.Fatalf("handler log line not found in %v", lines)
}
