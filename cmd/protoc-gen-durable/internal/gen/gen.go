// Package gen implements collection, validation, and code emission for
// protoc-gen-durable.
package gen

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/dangra/durable/durablepb"
)

const (
	durablePkg = protogen.GoImportPath("github.com/dangra/durable")
	contextPkg = protogen.GoImportPath("context")
	syncPkg    = protogen.GoImportPath("sync")
	protoPkg   = protogen.GoImportPath("google.golang.org/protobuf/proto")
)

type stepDecl struct {
	msg      *protogen.Message
	opts     *durablepb.StepOptions
	hasState bool
	owner    *pipelineDecl
}

type pipelineDecl struct {
	msg    *protogen.Message
	file   *protogen.File
	opts   *durablepb.PipelineOptions
	input  *protogen.Message // nil when the pipeline declares no Input
	output *protogen.Message // nil when the pipeline declares no Output
	steps  []*stepDecl
}

// Generate is the plugin entry point.
func Generate(p *protogen.Plugin) error {
	messages := indexMessages(p)

	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	steps := make(map[protoreflect.FullName]*stepDecl)
	var pipelines []*pipelineDecl
	// Declarations are collected only from files being generated.
	for _, f := range p.Files {
		if !f.Generate {
			continue
		}
		walkMessages(f.Messages, func(m *protogen.Message) {
			if so := stepOptions(m); so != nil {
				if so.GetId() == "" {
					fail("%s: step declaration missing id", m.Desc.FullName())
				}
				steps[m.Desc.FullName()] = &stepDecl{
					msg:      m,
					opts:     so,
					hasState: len(m.Fields) > 0,
				}
			}
			if po := pipelineOptions(m); po != nil {
				pipelines = append(pipelines, &pipelineDecl{msg: m, file: f, opts: po})
			}
		})
	}

	// Duplicate StepIDs across all declarations.
	byID := make(map[string]protoreflect.FullName)
	for name, s := range steps {
		id := s.opts.GetId()
		if id == "" {
			continue
		}
		if prev, dup := byID[id]; dup {
			fail("duplicate step id %q declared by %s and %s", id, prev, name)
		} else {
			byID[id] = name
		}
	}

	pipelineIDs := make(map[string]protoreflect.FullName)
	for _, pl := range pipelines {
		m := pl.msg
		if pl.opts.GetId() == "" {
			fail("%s: pipeline declaration missing id", m.Desc.FullName())
		} else if prev, dup := pipelineIDs[pl.opts.GetId()]; dup {
			fail("duplicate pipeline id %q declared by %s and %s", pl.opts.GetId(), prev, m.Desc.FullName())
		} else {
			pipelineIDs[pl.opts.GetId()] = m.Desc.FullName()
		}
		if len(m.Fields) > 0 {
			fail("%s: pipeline marker message must not declare fields", m.Desc.FullName())
		}
		if len(pl.opts.GetSteps()) == 0 {
			fail("%s: pipeline declares no steps", m.Desc.FullName())
		}

		if in := pl.opts.GetInput(); in != "" {
			msg, ok := messages[trimDot(in)]
			if !ok {
				fail("%s: input type %q not found", m.Desc.FullName(), in)
			} else {
				pl.input = msg
			}
		}
		if out := pl.opts.GetOutput(); out != "" {
			msg, ok := messages[trimDot(out)]
			if !ok {
				fail("%s: output type %q not found", m.Desc.FullName(), out)
			} else {
				pl.output = msg
			}
		}

		for _, ref := range pl.opts.GetSteps() {
			name := trimDot(ref)
			msg, ok := messages[name]
			if !ok {
				fail("%s: step %q not found", m.Desc.FullName(), ref)
				continue
			}
			sd, ok := steps[protoreflect.FullName(name)]
			if !ok {
				fail("%s: message %q in pipeline topology is not a durable step declaration", m.Desc.FullName(), ref)
				continue
			}
			_ = msg
			if sd.owner != nil {
				fail("step %s is declared by pipelines %q and %q; one durable step belongs to exactly one active pipeline",
					name, sd.owner.opts.GetId(), pl.opts.GetId())
				continue
			}
			sd.owner = pl
			pl.steps = append(pl.steps, sd)
		}
	}

	for name, s := range steps {
		if s.owner == nil {
			fail("step %s is not referenced by any pipeline", name)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	for _, pl := range pipelines {
		emitPipeline(p, pl)
	}
	return nil
}

func indexMessages(p *protogen.Plugin) map[string]*protogen.Message {
	idx := make(map[string]*protogen.Message)
	for _, f := range p.Files {
		walkMessages(f.Messages, func(m *protogen.Message) {
			idx[string(m.Desc.FullName())] = m
		})
	}
	return idx
}

func walkMessages(msgs []*protogen.Message, fn func(*protogen.Message)) {
	for _, m := range msgs {
		fn(m)
		walkMessages(m.Messages, fn)
	}
}

func stepOptions(m *protogen.Message) *durablepb.StepOptions {
	opts, ok := m.Desc.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return nil
	}
	if !proto.HasExtension(opts, durablepb.E_Step) {
		return nil
	}
	return proto.GetExtension(opts, durablepb.E_Step).(*durablepb.StepOptions)
}

func pipelineOptions(m *protogen.Message) *durablepb.PipelineOptions {
	opts, ok := m.Desc.Options().(*descriptorpb.MessageOptions)
	if !ok || opts == nil {
		return nil
	}
	if !proto.HasExtension(opts, durablepb.E_Pipeline) {
		return nil
	}
	return proto.GetExtension(opts, durablepb.E_Pipeline).(*durablepb.PipelineOptions)
}

func trimDot(s string) string { return strings.TrimPrefix(s, ".") }

func lowerFirst(s string) string {
	r, size := utf8.DecodeRuneInString(s)
	out := string(unicode.ToLower(r)) + s[size:]
	switch out {
	case "break", "case", "chan", "const", "continue", "default", "defer",
		"else", "fallthrough", "for", "func", "go", "goto", "if", "import",
		"interface", "map", "package", "range", "return", "select", "struct",
		"switch", "type", "var":
		return out + "_"
	}
	return out
}

// emitPipeline writes <pipeline-file>_durable.pb.go with the pipeline's
// typed API: step references, invocation types, handler interfaces, the
// definition constructor, the reducer plumbing, and the bound pipeline,
// run, and result types.
func emitPipeline(p *protogen.Plugin, pl *pipelineDecl) {
	f := pl.file
	name := pl.msg.GoIdent.GoName
	filename := f.GeneratedFilenamePrefix + "_durable.pb.go"
	g := p.NewGeneratedFile(filename, f.GoImportPath)

	g.P("// Code generated by protoc-gen-durable. DO NOT EDIT.")
	g.P("//")
	g.P("// source: ", f.Desc.Path())
	g.P("// pipeline: ", pl.opts.GetId())
	g.P()
	g.P("package ", f.GoPackageName)
	g.P()

	for _, s := range pl.steps {
		emitStepRef(g, s)
	}
	for _, s := range pl.steps {
		emitInvocation(g, pl, s)
		emitHandler(g, s)
	}
	if pl.output != nil {
		emitReducerType(g, pl)
	}
	emitMarkerView(g, pl)
	emitDefinition(g, pl)
	emitBoundPipeline(g, pl)
	if pl.output != nil {
		emitTypedRunAndResult(g, pl)
	}
	_ = name
}

func emitStepRef(g *protogen.GeneratedFile, s *stepDecl) {
	goName := s.msg.GoIdent.GoName
	id := s.opts.GetId()
	if s.hasState {
		g.P("// ", goName, "Step is the typed reference to the state-producing step ", strconv(id), ".")
		g.P("var ", goName, "Step = ", g.QualifiedGoIdent(durablePkg.Ident("NewStateStepRef")),
			"(", strconv(id), ", func() *", g.QualifiedGoIdent(s.msg.GoIdent), " { return &", g.QualifiedGoIdent(s.msg.GoIdent), "{} })")
	} else {
		g.P("// ", goName, "Step is the reference to the stateless step ", strconv(id), ".")
		g.P("// It is not accepted by State lookup.")
		g.P("var ", goName, "Step = ", g.QualifiedGoIdent(durablePkg.Ident("NewStepRef")), "(", strconv(id), ")")
	}
	g.P()
}

func emitInvocation(g *protogen.GeneratedFile, pl *pipelineDecl, s *stepDecl) {
	goName := s.msg.GoIdent.GoName
	inv := goName + "Invocation"
	core := g.QualifiedGoIdent(durablePkg.Ident("Invocation"))

	g.P("// ", inv, " is passed to ", goName, "Handler methods.")
	g.P("type ", inv, " struct {")
	g.P("core *", core)
	g.P("}")
	g.P()
	g.P("func (inv ", inv, ") PipelineID() ", g.QualifiedGoIdent(durablePkg.Ident("PipelineID")), " { return inv.core.PipelineID() }")
	g.P("func (inv ", inv, ") ResourceID() ", g.QualifiedGoIdent(durablePkg.Ident("ResourceID")), " { return inv.core.ResourceID() }")
	g.P("func (inv ", inv, ") RunID() ", g.QualifiedGoIdent(durablePkg.Ident("RunID")), " { return inv.core.RunID() }")
	g.P("func (inv ", inv, ") StepID() ", g.QualifiedGoIdent(durablePkg.Ident("StepID")), " { return inv.core.StepID() }")
	g.P("func (inv ", inv, ") Attempt() uint64 { return inv.core.Attempt() }")
	g.P("func (inv ", inv, ") Phase() ", g.QualifiedGoIdent(durablePkg.Ident("Phase")), " { return inv.core.Phase() }")
	g.P()
	if pl.input != nil {
		g.P("// Input returns a defensive caller-owned copy of the immutable pipeline input.")
		g.P("func (inv ", inv, ") Input() *", g.QualifiedGoIdent(pl.input.GoIdent), " {")
		g.P("msg, _ := inv.core.InputMessage().(*", g.QualifiedGoIdent(pl.input.GoIdent), ")")
		g.P("return msg")
		g.P("}")
		g.P()
	}
	g.P("// State returns the committed state of the referenced step for this run.")
	g.P("// ok is false when no committed state exists.")
	g.P("func (inv ", inv, ") State[T ", g.QualifiedGoIdent(protoPkg.Ident("Message")), "](step ", g.QualifiedGoIdent(durablePkg.Ident("StateStepRef")), "[T]) (T, bool) {")
	g.P("return ", g.QualifiedGoIdent(durablePkg.Ident("LookupState")), "(inv.core, step)")
	g.P("}")
	g.P()
}

func emitHandler(g *protogen.GeneratedFile, s *stepDecl) {
	goName := s.msg.GoIdent.GoName
	inv := goName + "Invocation"
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))

	g.P("// ", goName, "Handler implements step ", strconv(s.opts.GetId()), ".")
	g.P("type ", goName, "Handler interface {")
	if s.hasState {
		g.P("Run(", ctx, ", ", inv, ") (*", g.QualifiedGoIdent(s.msg.GoIdent), ", error)")
	} else {
		g.P("Run(", ctx, ", ", inv, ") error")
	}
	if s.opts.GetUnwind() {
		g.P()
		g.P("Unwind(", ctx, ", ", inv, ", ", g.QualifiedGoIdent(durablePkg.Ident("Failure")), ") error")
	}
	g.P("}")
	g.P()
	emitHandlerFuncs(g, s)
}

// emitHandlerFuncs emits http.HandlerFunc-style adapters: a func type for
// single-method handler interfaces, a struct of funcs for unwind-bearing
// ones (a func type cannot implement a two-method interface).
func emitHandlerFuncs(g *protogen.GeneratedFile, s *stepDecl) {
	goName := s.msg.GoIdent.GoName
	inv := goName + "Invocation"
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))

	runSig := "(ctx " + ctx + ", inv " + inv + ") "
	if s.hasState {
		runSig += "(*" + g.QualifiedGoIdent(s.msg.GoIdent) + ", error)"
	} else {
		runSig += "error"
	}

	if !s.opts.GetUnwind() {
		g.P("// ", goName, "Func adapts a function to ", goName, "Handler, in the style of")
		g.P("// http.HandlerFunc.")
		g.P("type ", goName, "Func func", runSig)
		g.P()
		g.P("func (f ", goName, "Func) Run", runSig, " { return f(ctx, inv) }")
		g.P()
		return
	}

	failure := g.QualifiedGoIdent(durablePkg.Ident("Failure"))
	g.P("// ", goName, "Funcs adapts a pair of functions to ", goName, "Handler.")
	g.P("type ", goName, "Funcs struct {")
	g.P("RunFunc func", runSig)
	g.P("UnwindFunc func(", ctx, ", ", inv, ", ", failure, ") error")
	g.P("}")
	g.P()
	g.P("func (f ", goName, "Funcs) Run", runSig, " { return f.RunFunc(ctx, inv) }")
	g.P()
	g.P("func (f ", goName, "Funcs) Unwind(ctx ", ctx, ", inv ", inv, ", failure ", failure, ") error {")
	g.P("return f.UnwindFunc(ctx, inv, failure)")
	g.P("}")
	g.P()
}

func emitReducerType(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	g.P("// ", name, "Reducer produces the pipeline output from the immutable input")
	g.P("// and committed step states. It must be pure: deterministic, side-effect")
	g.P("// free, synchronous, and non-failing.")
	g.P("type ", name, "Reducer func(*", g.QualifiedGoIdent(pl.msg.GoIdent), ") *", g.QualifiedGoIdent(pl.output.GoIdent))
	g.P()
}

func emitMarkerView(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	views := lowerFirst(name) + "Views"
	view := g.QualifiedGoIdent(durablePkg.Ident("ReduceView"))

	g.P("var ", views, " ", g.QualifiedGoIdent(syncPkg.Ident("Map")))
	g.P()
	g.P("func (x *", name, ") durableView() *", view, " {")
	g.P("v, ok := ", views, ".Load(x)")
	g.P("if !ok {")
	g.P("panic(", strconv(string(pl.file.GoPackageName)+": "+name+" is only usable as a reducer view during reduction"), ")")
	g.P("}")
	g.P("return v.(*", view, ")")
	g.P("}")
	g.P()
	if pl.input != nil {
		g.P("// Input returns a defensive caller-owned copy of the immutable pipeline input.")
		g.P("func (x *", name, ") Input() *", g.QualifiedGoIdent(pl.input.GoIdent), " {")
		g.P("msg, _ := x.durableView().InputMessage().(*", g.QualifiedGoIdent(pl.input.GoIdent), ")")
		g.P("return msg")
		g.P("}")
		g.P()
	}
	g.P("// State returns the committed state of the referenced step for the run")
	g.P("// being reduced. ok is false when no committed state exists.")
	g.P("func (x *", name, ") State[T ", g.QualifiedGoIdent(protoPkg.Ident("Message")), "](step ", g.QualifiedGoIdent(durablePkg.Ident("StateStepRef")), "[T]) (T, bool) {")
	g.P("return ", g.QualifiedGoIdent(durablePkg.Ident("LookupState")), "(x.durableView(), step)")
	g.P("}")
	g.P()
}

func emitDefinition(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	views := lowerFirst(name) + "Views"
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))
	core := g.QualifiedGoIdent(durablePkg.Ident("Invocation"))
	protoMsg := g.QualifiedGoIdent(protoPkg.Ident("Message"))

	g.P("// ", name, "Definition is the unbound pipeline definition.")
	g.P("type ", name, "Definition struct {")
	g.P("def *", g.QualifiedGoIdent(durablePkg.Ident("Definition")))
	g.P("}")
	g.P()

	g.P("// New", name, " assembles the ", strconv(pl.opts.GetId()), " pipeline definition")
	g.P("// from its step handlers", func() string {
		if pl.output != nil {
			return " and reducer"
		}
		return ""
	}(), ".")
	g.P("func New", name, "(")
	for _, s := range pl.steps {
		g.P(lowerFirst(s.msg.GoIdent.GoName), " ", s.msg.GoIdent.GoName, "Handler,")
	}
	if pl.output != nil {
		g.P("reduce ", name, "Reducer,")
	}
	g.P(") *", name, "Definition {")
	g.P("return &", name, "Definition{def: ", g.QualifiedGoIdent(durablePkg.Ident("NewDefinition")), "(", g.QualifiedGoIdent(durablePkg.Ident("DefinitionConfig")), "{")
	g.P("ID: ", strconv(pl.opts.GetId()), ",")
	if pl.input != nil {
		g.P("NewInput: func() ", protoMsg, " { return &", g.QualifiedGoIdent(pl.input.GoIdent), "{} },")
	}
	if pl.output != nil {
		g.P("Reduce: func(view *", g.QualifiedGoIdent(durablePkg.Ident("ReduceView")), ") ", protoMsg, " {")
		g.P("x := &", g.QualifiedGoIdent(pl.msg.GoIdent), "{}")
		g.P(views, ".Store(x, view)")
		g.P("defer ", views, ".Delete(x)")
		g.P("return reduce(x)")
		g.P("},")
	}
	g.P("Steps: []", g.QualifiedGoIdent(durablePkg.Ident("StepConfig")), "{")
	for _, s := range pl.steps {
		goName := s.msg.GoIdent.GoName
		param := lowerFirst(goName)
		g.P("{")
		g.P("ID: ", strconv(s.opts.GetId()), ",")
		if s.opts.GetUnwind() {
			g.P("Unwind: true,")
		}
		if s.opts.GetRetired() {
			g.P("Retired: true,")
		}
		if s.hasState {
			g.P("HasState: true,")
			g.P("Run: func(ctx ", ctx, ", core *", core, ") (", protoMsg, ", error) {")
			g.P("state, err := ", param, ".Run(ctx, ", goName, "Invocation{core: core})")
			g.P("if state == nil {")
			g.P("return nil, err")
			g.P("}")
			g.P("return state, err")
			g.P("},")
		} else {
			g.P("Run: func(ctx ", ctx, ", core *", core, ") (", protoMsg, ", error) {")
			g.P("return nil, ", param, ".Run(ctx, ", goName, "Invocation{core: core})")
			g.P("},")
		}
		if s.opts.GetUnwind() {
			g.P("UnwindFunc: func(ctx ", ctx, ", core *", core, ", failure ", g.QualifiedGoIdent(durablePkg.Ident("Failure")), ") error {")
			g.P("return ", param, ".Unwind(ctx, ", goName, "Invocation{core: core}, failure)")
			g.P("},")
		}
		g.P("},")
	}
	g.P("},")
	g.P("})}")
	g.P("}")
	g.P()

	g.P("// Bind registers the definition with an engine. It is allowed only before")
	g.P("// Engine.Start.")
	g.P("func (d *", name, "Definition) Bind(e *", g.QualifiedGoIdent(durablePkg.Ident("Engine")), ") (*", name, "Pipeline, error) {")
	g.P("p, err := d.def.Bind(e)")
	g.P("if err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return &", name, "Pipeline{pipeline: p}, nil")
	g.P("}")
	g.P()
}

func emitBoundPipeline(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))
	resourceID := g.QualifiedGoIdent(durablePkg.Ident("ResourceID"))
	runID := g.QualifiedGoIdent(durablePkg.Ident("RunID"))
	plainRun := g.QualifiedGoIdent(durablePkg.Ident("Run"))

	runType := plainRun
	wrap := func(expr string) string { return expr }
	if pl.output != nil {
		runType = name + "Run"
		wrap = func(expr string) string { return name + "Run{run: " + expr + "}" }
	}

	g.P("// ", name, "Pipeline is the definition bound to an engine.")
	g.P("type ", name, "Pipeline struct {")
	g.P("pipeline *", g.QualifiedGoIdent(durablePkg.Ident("Pipeline")))
	g.P("}")
	g.P()

	scheduleOpt := g.QualifiedGoIdent(durablePkg.Ident("ScheduleOption"))
	g.P("// Schedule creates a run for the resource slot or returns the active one.")
	if pl.input != nil {
		g.P("func (p *", name, "Pipeline) Schedule(ctx ", ctx, ", resource ", resourceID, ", input *", g.QualifiedGoIdent(pl.input.GoIdent), ", opts ...", scheduleOpt, ") (", runType, ", bool, error) {")
		g.P("run, created, err := p.pipeline.Schedule(ctx, resource, input, opts...)")
	} else {
		g.P("func (p *", name, "Pipeline) Schedule(ctx ", ctx, ", resource ", resourceID, ", opts ...", scheduleOpt, ") (", runType, ", bool, error) {")
		g.P("run, created, err := p.pipeline.Schedule(ctx, resource, nil, opts...)")
	}
	if pl.output != nil {
		g.P("if err != nil {")
		g.P("return ", runType, "{}, created, err")
		g.P("}")
		g.P("return ", wrap("run"), ", created, nil")
	} else {
		g.P("return run, created, err")
	}
	g.P("}")
	g.P()

	g.P("// Run returns a handle to an existing run of this pipeline.")
	g.P("func (p *", name, "Pipeline) Run(ctx ", ctx, ", id ", runID, ") (", runType, ", error) {")
	g.P("run, err := p.pipeline.Run(ctx, id)")
	if pl.output != nil {
		g.P("if err != nil {")
		g.P("return ", runType, "{}, err")
		g.P("}")
		g.P("return ", wrap("run"), ", nil")
	} else {
		g.P("return run, err")
	}
	g.P("}")
	g.P()

	g.P("// Active returns handles for this pipeline's nonterminal runs.")
	g.P("func (p *", name, "Pipeline) Active(ctx ", ctx, ") ([]", runType, ", error) {")
	g.P("runs, err := p.pipeline.Active(ctx)")
	emitRunSliceWrap(g, pl, runType, wrap)
	g.P("}")
	g.P()

	g.P("// Runs returns handles for all runs of this pipeline against a resource,")
	g.P("// oldest first.")
	g.P("func (p *", name, "Pipeline) Runs(ctx ", ctx, ", resource ", resourceID, ") ([]", runType, ", error) {")
	g.P("runs, err := p.pipeline.Runs(ctx, resource)")
	emitRunSliceWrap(g, pl, runType, wrap)
	g.P("}")
	g.P()
}

func emitRunSliceWrap(g *protogen.GeneratedFile, pl *pipelineDecl, runType string, wrap func(string) string) {
	if pl.output == nil {
		g.P("return runs, err")
		return
	}
	g.P("if err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("out := make([]", runType, ", 0, len(runs))")
	g.P("for _, run := range runs {")
	g.P("out = append(out, ", wrap("run"), ")")
	g.P("}")
	g.P("return out, nil")
}

func emitTypedRunAndResult(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))

	g.P("// ", name, "Run is a typed handle to one run of the pipeline.")
	g.P("type ", name, "Run struct {")
	g.P("run ", g.QualifiedGoIdent(durablePkg.Ident("Run")))
	g.P("}")
	g.P()
	g.P("func (r ", name, "Run) ID() ", g.QualifiedGoIdent(durablePkg.Ident("RunID")), " { return r.run.ID() }")
	g.P()
	g.P("func (r ", name, "Run) Status(ctx ", ctx, ") (", g.QualifiedGoIdent(durablePkg.Ident("Status")), ", error) {")
	g.P("return r.run.Status(ctx)")
	g.P("}")
	g.P()
	g.P("// Cancel durably requests cancellation: the run stops selecting new")
	g.P("// forward work and unwinds successfully executed steps.")
	g.P("func (r ", name, "Run) Cancel(ctx ", ctx, ", cause string) error {")
	g.P("return r.run.Cancel(ctx, cause)")
	g.P("}")
	g.P()
	g.P("// Wait blocks until the run is terminal. A successful result carries the")
	g.P("// pipeline output; a failed run has none.")
	g.P("func (r ", name, "Run) Wait(ctx ", ctx, ") (", name, "Result, error) {")
	g.P("res, err := r.run.Wait(ctx)")
	g.P("if err != nil {")
	g.P("return ", name, "Result{}, err")
	g.P("}")
	g.P("out := ", name, "Result{Result: res}")
	g.P("if res.Succeeded() {")
	g.P("b, err := r.run.OutputBytes(ctx)")
	g.P("if err != nil {")
	g.P("return ", name, "Result{}, err")
	g.P("}")
	g.P("msg := &", g.QualifiedGoIdent(pl.output.GoIdent), "{}")
	g.P("if err := ", g.QualifiedGoIdent(protoPkg.Ident("Unmarshal")), "(b, msg); err != nil {")
	g.P("return ", name, "Result{}, err")
	g.P("}")
	g.P("out.output = msg")
	g.P("}")
	g.P("return out, nil")
	g.P("}")
	g.P()
	g.P("// ", name, "Result is the typed terminal result of a run.")
	g.P("type ", name, "Result struct {")
	g.P(g.QualifiedGoIdent(durablePkg.Ident("Result")))
	g.P("output *", g.QualifiedGoIdent(pl.output.GoIdent))
	g.P("}")
	g.P()
	g.P("// Output returns the pipeline output. It is non-nil exactly when the run")
	g.P("// succeeded.")
	g.P("func (r ", name, "Result) Output() *", g.QualifiedGoIdent(pl.output.GoIdent), " { return r.output }")
	g.P()
}

func strconv(s string) string { return fmt.Sprintf("%q", s) }
