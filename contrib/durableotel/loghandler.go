package durableotel

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// NewLogHandler wraps a slog.Handler so every record logged with a
// context carrying a span — the ctx Middleware hands to handlers —
// gains trace_id, span_id, and trace_flags (hex-encoded W3C flags)
// attributes, the OpenTelemetry log-correlation convention. Wrap the
// handler behind the engine's WithLogger once:
//
//	logger := slog.New(durableotel.NewLogHandler(
//		slog.NewJSONHandler(os.Stderr, nil)))
//	engine := durable.NewEngine(store, durable.WithLogger(logger),
//		durable.WithMiddleware(durableotel.Middleware()))
//
// and handler lines join their attempt span, on top of the canonical
// durable keys Invocation.Logger already attaches:
//
//	inv.Logger().InfoContext(ctx, "charging card")
//
// The context variants (InfoContext, not Info) are what carry the span;
// a record logged without one passes through unchanged, as do the
// engine's own lifecycle lines — those already correlate by run ID. The
// wrapper is engine-agnostic: any application log call whose ctx holds
// a span is stamped, so one wrapped logger serves the whole process.
//
// Correlation attributes join the record at log time, so under an open
// WithGroup they land in that group like any other record attribute;
// keep grouping below this wrapper out of correlation-critical loggers.
func NewLogHandler(inner slog.Handler) slog.Handler {
	return &logHandler{inner: inner}
}

type logHandler struct {
	inner slog.Handler
}

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone() // the caller may share the record with other handlers
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
			slog.String("trace_flags", sc.TraceFlags().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{inner: h.inner.WithGroup(name)}
}
