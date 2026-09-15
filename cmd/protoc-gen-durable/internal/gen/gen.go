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
	enginePkg  = protogen.GoImportPath("github.com/dangra/durable/engine")
	defPkg     = protogen.GoImportPath("github.com/dangra/durable/pipelinedef")
	contextPkg = protogen.GoImportPath("context")
	slogPkg    = protogen.GoImportPath("log/slog")
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
	msg           *protogen.Message
	file          *protogen.File
	opts          *durablepb.PipelineOptions
	input         *protogen.Message // nil when the pipeline declares no Input
	output        *protogen.Message // nil when the pipeline declares no Output
	failureOutput *protogen.Message // nil when the pipeline declares no failure Output
	steps         []*stepDecl
}

// Generate is the plugin entry point.
func Generate(p *protogen.Plugin) error {
	messages := indexMessages(p)

	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}

	// Declarations are collected only from files being generated. A
	// pipeline is a top-level message; its steps are the messages nested
	// directly in it, in declaration order, which is the topology. A
	// step anywhere else, or a pipeline nested anywhere, is an error.
	var pipelines []*pipelineDecl
	byID := make(map[string]protoreflect.FullName)
	for _, f := range p.Files {
		if !f.Generate {
			continue
		}
		for _, m := range f.Messages {
			if so := stepOptions(m); so != nil {
				fail("%s: a step must be nested in its pipeline message", m.Desc.FullName())
			}
			po := pipelineOptions(m)
			if po == nil {
				walkMessages(m.Messages, func(n *protogen.Message) {
					if stepOptions(n) != nil {
						fail("%s: a step must be nested directly in a pipeline message", n.Desc.FullName())
					}
					if pipelineOptions(n) != nil {
						fail("%s: a pipeline must be a top-level message", n.Desc.FullName())
					}
				})
				continue
			}
			pl := &pipelineDecl{msg: m, file: f, opts: po}
			pipelines = append(pipelines, pl)
			for _, n := range m.Messages {
				so := stepOptions(n)
				if so == nil {
					fail("%s: every message nested in a pipeline is a step; %s declares no step option", m.Desc.FullName(), n.Desc.Name())
					continue
				}
				if pipelineOptions(n) != nil {
					fail("%s: a pipeline must be a top-level message", n.Desc.FullName())
				}
				walkMessages(n.Messages, func(x *protogen.Message) {
					if stepOptions(x) != nil || pipelineOptions(x) != nil {
						fail("%s: a message nested in a step is plain data, not a step or pipeline", x.Desc.FullName())
					}
				})
				id := so.GetId()
				if id == "" {
					fail("%s: step declaration missing id", n.Desc.FullName())
				} else if prev, dup := byID[id]; dup {
					fail("duplicate step id %q declared by %s and %s", id, prev, n.Desc.FullName())
				} else {
					byID[id] = n.Desc.FullName()
				}
				pl.steps = append(pl.steps, &stepDecl{msg: n, opts: so, hasState: len(n.Fields) > 0, owner: pl})
			}
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
		if len(pl.steps) == 0 {
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
		if fo := pl.opts.GetFailureOutput(); fo != "" {
			msg, ok := messages[trimDot(fo)]
			if !ok {
				fail("%s: failure_output type %q not found", m.Desc.FullName(), fo)
			} else {
				pl.failureOutput = msg
			}
		}
	}

	// Every step is a method on its pipeline's handler interface, so the
	// names a pipeline's steps and reducers take must not collide.
	for _, pl := range pipelines {
		methods := map[string]string{}
		claim := func(method, by string) {
			if prev, dup := methods[method]; dup {
				fail("%s: handler method %s is claimed by both %s and %s", pl.msg.Desc.FullName(), method, prev, by)
			}
			methods[method] = by
		}
		if pl.output != nil {
			claim("Reduce", "the output reducer")
		}
		if pl.failureOutput != nil {
			claim("ReduceFailure", "the failure reducer")
		}
		for _, s := range pl.steps {
			claim(s.methodName(), "step "+s.opts.GetId())
			if s.opts.GetUnwind() {
				claim("Unwind"+s.methodName(), "the unwind of step "+s.opts.GetId())
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	// One generated file per proto file, holding all of its pipelines.
	byFile := make(map[*protogen.File][]*pipelineDecl)
	var fileOrder []*protogen.File
	for _, pl := range pipelines {
		if _, seen := byFile[pl.file]; !seen {
			fileOrder = append(fileOrder, pl.file)
		}
		byFile[pl.file] = append(byFile[pl.file], pl)
	}
	for _, f := range fileOrder {
		emitFile(p, f, byFile[f])
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

// emitFile writes <proto-file>_durable.pb.go with the typed API of every
// pipeline the proto file declares: step references, invocation types,
// handler interfaces, definition constructors, reducer plumbing, and the
// bound pipeline, run, and result types.
func emitFile(p *protogen.Plugin, f *protogen.File, pipelines []*pipelineDecl) {
	filename := f.GeneratedFilenamePrefix + "_durable.pb.go"
	g := p.NewGeneratedFile(filename, f.GoImportPath)

	g.P("// Code generated by protoc-gen-durable. DO NOT EDIT.")
	g.P("//")
	g.P("// source: ", f.Desc.Path())
	for _, pl := range pipelines {
		g.P("// pipeline: ", pl.opts.GetId())
	}
	g.P()
	g.P("package ", f.GoPackageName)
	g.P()

	for _, pl := range pipelines {
		emitPipeline(g, pl)
	}
}

func emitPipeline(g *protogen.GeneratedFile, pl *pipelineDecl) {
	for _, s := range pl.steps {
		emitStepRef(g, s)
	}
	emitInvocationAlias(g, pl)
	emitHandlers(g, pl)
	if pl.output != nil {
		emitReduceHelper(g, pl, "", pl.output)
	}
	if pl.failureOutput != nil {
		emitReduceHelper(g, pl, "Failure", pl.failureOutput)
	}
	emitMarkerView(g, pl)
	emitDefinition(g, pl)
	emitBoundPipeline(g, pl)
	if pl.typedRun() {
		emitTypedRunAndResult(g, pl)
	}
}

// typedRun reports whether the pipeline gets a generated typed Run: any
// pipeline with an Input (typed Input accessor) or an Output or failure
// Output (typed Wait result).
func (pl *pipelineDecl) typedRun() bool {
	return pl.input != nil || pl.typedResult()
}

// typedResult reports whether Wait returns a typed Result.
func (pl *pipelineDecl) typedResult() bool {
	return pl.output != nil || pl.failureOutput != nil
}

func emitStepRef(g *protogen.GeneratedFile, s *stepDecl) {
	goName := s.msg.GoIdent.GoName
	id := s.opts.GetId()
	if s.hasState {
		g.P("// ", goName, "Step is the typed reference to the state-producing step ", strconv(id), ".")
		g.P("var ", goName, "Step = ", g.QualifiedGoIdent(defPkg.Ident("StateStepRef")),
			"(", strconv(id), ", func() *", g.QualifiedGoIdent(s.msg.GoIdent), " { return &", g.QualifiedGoIdent(s.msg.GoIdent), "{} })")
	} else {
		g.P("// ", goName, "Step is the reference to the stateless step ", strconv(id), ".")
		g.P("// It is not accepted by State lookup.")
		g.P("var ", goName, "Step = ", g.QualifiedGoIdent(defPkg.Ident("StepRef")), "(", strconv(id), ")")
	}
	g.P()
}

// invocationName is the pipeline's Invocation alias.
func (pl *pipelineDecl) invocationName() string { return pl.msg.GoIdent.GoName + "Invocation" }

// handlersName is the pipeline's handler interface.
func (pl *pipelineDecl) handlersName() string { return pl.msg.GoIdent.GoName + "Handlers" }

// inputType is the Go expression of the pipeline's Input type parameter.
func (pl *pipelineDecl) inputType(g *protogen.GeneratedFile) string {
	if pl.input == nil {
		return g.QualifiedGoIdent(durablePkg.Ident("NoInput"))
	}
	return "*" + g.QualifiedGoIdent(pl.input.GoIdent)
}

// emitInvocationAlias names the pipeline's typed invocation: one alias
// per pipeline, shared by every handler method.
func emitInvocationAlias(g *protogen.GeneratedFile, pl *pipelineDecl) {
	inv := pl.invocationName()
	g.P("// ", inv, " is the invocation every ", pl.handlersName(), " method receives:")
	g.P("// durable.Invocation with the pipeline's Input typed.")
	g.P("type ", inv, " = ", g.QualifiedGoIdent(durablePkg.Ident("TypedInvocation")), "[", pl.inputType(g), "]")
	g.P()
	g.P("// New", inv, " wraps core for calling a ", pl.handlersName(), " method directly,")
	g.P("// outside an engine: hand it durabletest.NewInvocation to unit-test a")
	g.P("// handler. The engine wraps its own invocations; application code never")
	g.P("// needs this at runtime.")
	g.P("func New", inv, "(core ", g.QualifiedGoIdent(durablePkg.Ident("Invocation")), ") ", inv, " {")
	g.P("return ", g.QualifiedGoIdent(durablePkg.Ident("Typed")), "[", pl.inputType(g), "](core)")
	g.P("}")
	g.P()
}

// methodName is the step's handler method: the nested message's own
// name. Its Go type is Pipeline_Step, protoc-gen-go's nested naming.
func (s *stepDecl) methodName() string { return string(s.msg.Desc.Name()) }

// runSig is the signature of a step's forward method, after the name.
func (s *stepDecl) runSig(g *protogen.GeneratedFile, inv string) string {
	sig := "(ctx " + g.QualifiedGoIdent(contextPkg.Ident("Context")) + ", inv " + inv + ") "
	if s.hasState {
		return sig + "(*" + g.QualifiedGoIdent(s.msg.GoIdent) + ", error)"
	}
	return sig + "error"
}

// emitHandlers emits the pipeline's handler interface: one method per
// step, named after the step message, an Unwind<Step> method for each
// step that unwinds, and the reducers the pipeline declares.
func emitHandlers(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	inv := pl.invocationName()
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))

	g.P("// ", pl.handlersName(), " implements the ", strconv(pl.opts.GetId()), " pipeline: one method per")
	g.P("// step, named after the step, plus Unwind<Step> for each step that")
	g.P("// unwinds. A type implements every step or does not compile; a step")
	g.P("// added to the pipeline is a method the next build demands.")
	g.P("type ", pl.handlersName(), " interface {")
	for _, s := range pl.steps {
		method := s.methodName()
		g.P("// ", method, " runs step ", strconv(s.opts.GetId()), ".")
		g.P(method, s.runSig(g, inv))
		if s.opts.GetUnwind() {
			g.P("// Unwind", method, " compensates step ", strconv(s.opts.GetId()), " once it")
			g.P("// succeeded and the run unwinds.")
			g.P("Unwind", method, "(ctx ", ctx, ", inv ", inv, ") error")
		}
	}
	if pl.output != nil {
		g.P("// Reduce produces the pipeline output from the immutable input and")
		g.P("// committed step states on success. It must be pure: deterministic,")
		g.P("// side-effect free, synchronous, and non-failing.")
		g.P("Reduce(*", g.QualifiedGoIdent(pl.msg.GoIdent), ") *", g.QualifiedGoIdent(pl.output.GoIdent))
	}
	if pl.failureOutput != nil {
		g.P("// ReduceFailure produces the pipeline failure output from the immutable")
		g.P("// input, the committed step states, the run's Failure, and the permanent")
		g.P("// unwind failures once the unwind completes. It must be pure.")
		g.P("ReduceFailure(*", g.QualifiedGoIdent(pl.msg.GoIdent), ") *", g.QualifiedGoIdent(pl.failureOutput.GoIdent))
	}
	g.P("}")
	g.P()
	_ = name
}

// emitReduceHelper emits Reduce<Pipeline>[Failure](h, view): the fold the
// engine reduces through and a reducer unit test calls with a
// durabletest.NewInvocation (which is also a durable.ReduceView).
func emitReduceHelper(g *protogen.GeneratedFile, pl *pipelineDecl, kind string, out *protogen.Message) {
	name := pl.msg.GoIdent.GoName
	views := lowerFirst(name) + "Views"
	fn := "Reduce" + name + kind
	g.P("// ", fn, " folds view through h.Reduce", kind, ": the marker the reducer")
	g.P("// receives reads its Input, States, and failures from view for the")
	g.P("// duration of the call.")
	g.P("func ", fn, "(h ", pl.handlersName(), ", view ", g.QualifiedGoIdent(durablePkg.Ident("ReduceView")), ") *", g.QualifiedGoIdent(out.GoIdent), " {")
	g.P("x := &", g.QualifiedGoIdent(pl.msg.GoIdent), "{}")
	g.P(views, ".Store(x, view)")
	g.P("defer ", views, ".Delete(x)")
	g.P("return h.Reduce", kind, "(x)")
	g.P("}")
	g.P()
}

func emitMarkerView(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	views := lowerFirst(name) + "Views"
	view := g.QualifiedGoIdent(durablePkg.Ident("ReduceView"))

	g.P("var ", views, " ", g.QualifiedGoIdent(syncPkg.Ident("Map")))
	g.P()
	g.P("func (x *", name, ") durableView() ", view, " {")
	g.P("v, ok := ", views, ".Load(x)")
	g.P("if !ok {")
	g.P("panic(", strconv(string(pl.file.GoPackageName)+": "+name+" is only usable as a reducer view during reduction"), ")")
	g.P("}")
	g.P("return v.(", view, ")")
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
	g.P("// Failure is the run's failure when a failed run is being reduced, nil")
	g.P("// when a successful one is.")
	g.P("func (x *", name, ") Failure() *", g.QualifiedGoIdent(durablePkg.Ident("Failure")), " { return x.durableView().Failure() }")
	g.P()
	g.P("// UnwindFailure reports the permanent failure of the referenced step's")
	g.P("// unwind, if its compensation failed; ok is false when the step was not")
	g.P("// unwound or its unwind succeeded. Pair it with State to describe what a")
	g.P("// failed run left behind.")
	g.P("func (x *", name, ") UnwindFailure(step ", g.QualifiedGoIdent(durablePkg.Ident("StepIdentifier")), ") (", g.QualifiedGoIdent(durablePkg.Ident("Failure")), ", bool) {")
	g.P("return x.durableView().UnwindFailure(step.ID())")
	g.P("}")
	g.P()
}

func emitDefinition(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))
	core := g.QualifiedGoIdent(durablePkg.Ident("Invocation"))
	protoMsg := g.QualifiedGoIdent(protoPkg.Ident("Message"))

	g.P("// ", name, "Definition is the unbound pipeline definition.")
	g.P("type ", name, "Definition struct {")
	g.P("def *", g.QualifiedGoIdent(defPkg.Ident("Definition")))
	g.P("}")
	g.P()

	g.P("// New", name, " assembles the ", strconv(pl.opts.GetId()), " pipeline definition")
	g.P("// from its handlers.")
	g.P("func New", name, "(h ", pl.handlersName(), ") *", name, "Definition {")
	g.P("return &", name, "Definition{def: ", g.QualifiedGoIdent(defPkg.Ident("New")), "(", g.QualifiedGoIdent(defPkg.Ident("Config")), "{")
	g.P("ID: ", strconv(pl.opts.GetId()), ",")
	if ms := pl.opts.GetMutexes(); len(ms) > 0 {
		quoted := make([]string, len(ms))
		for i, m := range ms {
			quoted[i] = strconv(m)
		}
		g.P("Mutexes: []string{", strings.Join(quoted, ", "), "},")
	}
	if cc := pl.opts.GetConcurrencyClass(); cc != "" {
		g.P("ConcurrencyClass: ", strconv(cc), ",")
	}
	if rc := pl.opts.GetRunClass(); rc != "" {
		g.P("RunClass: ", strconv(rc), ",")
	}
	if pl.input != nil {
		g.P("NewInput: func() ", protoMsg, " { return &", g.QualifiedGoIdent(pl.input.GoIdent), "{} },")
	}
	if pl.output != nil {
		g.P("Reduce: func(view ", g.QualifiedGoIdent(durablePkg.Ident("ReduceView")), ") ", protoMsg, " {")
		g.P("return Reduce", name, "(h, view)")
		g.P("},")
	}
	if pl.failureOutput != nil {
		g.P("ReduceFailure: func(view ", g.QualifiedGoIdent(durablePkg.Ident("ReduceView")), ") ", protoMsg, " {")
		g.P("return Reduce", name, "Failure(h, view)")
		g.P("},")
	}
	g.P("Steps: []", g.QualifiedGoIdent(defPkg.Ident("Step")), "{")
	typed := g.QualifiedGoIdent(durablePkg.Ident("Typed")) + "[" + pl.inputType(g) + "]"
	for _, s := range pl.steps {
		goName := s.methodName()
		g.P("{")
		g.P("ID: ", strconv(s.opts.GetId()), ",")
		if s.opts.GetUnwind() {
			g.P("Unwind: true,")
		}
		if s.opts.GetRetired() {
			g.P("Retired: true,")
		}
		if cc := s.opts.GetConcurrencyClass(); cc != "" {
			g.P("ConcurrencyClass: ", strconv(cc), ",")
		}
		if s.hasState {
			g.P("HasState: true,")
			g.P("Run: func(ctx ", ctx, ", core ", core, ") (", protoMsg, ", error) {")
			g.P("state, err := h.", goName, "(ctx, ", typed, "(core))")
			g.P("if state == nil {")
			g.P("return nil, err")
			g.P("}")
			g.P("return state, err")
			g.P("},")
		} else {
			g.P("Run: func(ctx ", ctx, ", core ", core, ") (", protoMsg, ", error) {")
			g.P("return nil, h.", goName, "(ctx, ", typed, "(core))")
			g.P("},")
		}
		if s.opts.GetUnwind() {
			g.P("UnwindFunc: func(ctx ", ctx, ", core ", core, ") error {")
			g.P("return h.Unwind", goName, "(ctx, ", typed, "(core))")
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
	g.P("func (d *", name, "Definition) Bind(e *", g.QualifiedGoIdent(enginePkg.Ident("Engine")), ") (*", name, "Pipeline, error) {")
	g.P("p, err := e.Bind(d.def)")
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
	plainRun := g.QualifiedGoIdent(enginePkg.Ident("Run"))

	runType := plainRun
	wrap := func(expr string) string { return expr }
	if pl.typedRun() {
		runType = name + "Run"
		wrap = func(expr string) string { return name + "Run{run: " + expr + "}" }
	}

	g.P("// ", name, "Pipeline is the definition bound to an engine.")
	g.P("type ", name, "Pipeline struct {")
	g.P("pipeline *", g.QualifiedGoIdent(enginePkg.Ident("Pipeline")))
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
	if pl.typedRun() {
		g.P("if err != nil {")
		g.P("return ", runType, "{}, created, err")
		g.P("}")
		g.P("return ", wrap("run"), ", created, nil")
	} else {
		g.P("return run, created, err")
	}
	g.P("}")
	g.P()

	g.P("// GetRun returns a handle to an existing run of this pipeline.")
	g.P("func (p *", name, "Pipeline) GetRun(ctx ", ctx, ", id ", runID, ") (", runType, ", error) {")
	g.P("run, err := p.pipeline.GetRun(ctx, id)")
	if pl.typedRun() {
		g.P("if err != nil {")
		g.P("return ", runType, "{}, err")
		g.P("}")
		g.P("return ", wrap("run"), ", nil")
	} else {
		g.P("return run, err")
	}
	g.P("}")
	g.P()

	g.P("// GetActiveRun returns a handle to this pipeline's nonterminal run for a")
	g.P("// resource, if one exists — a read-only observation for wait/inspect")
	g.P("// flows; claiming the slot atomically remains Schedule's job.")
	g.P("func (p *", name, "Pipeline) GetActiveRun(ctx ", ctx, ", resource ", resourceID, ") (", runType, ", bool, error) {")
	g.P("run, ok, err := p.pipeline.GetActiveRun(ctx, resource)")
	if pl.typedRun() {
		g.P("if err != nil || !ok {")
		g.P("return ", runType, "{}, ok, err")
		g.P("}")
		g.P("return ", wrap("run"), ", true, nil")
	} else {
		g.P("return run, ok, err")
	}
	g.P("}")
	g.P()
}

func emitTypedRunAndResult(g *protogen.GeneratedFile, pl *pipelineDecl) {
	name := pl.msg.GoIdent.GoName
	ctx := g.QualifiedGoIdent(contextPkg.Ident("Context"))

	g.P("// ", name, "Run is a typed handle to one run of the pipeline.")
	g.P("type ", name, "Run struct {")
	g.P("run ", g.QualifiedGoIdent(enginePkg.Ident("Run")))
	g.P("}")
	g.P()
	g.P("func (r ", name, "Run) ID() ", g.QualifiedGoIdent(durablePkg.Ident("RunID")), " { return r.run.ID() }")
	g.P()
	g.P("func (r ", name, "Run) Status(ctx ", ctx, ") (", g.QualifiedGoIdent(enginePkg.Ident("Status")), ", error) {")
	g.P("return r.run.Status(ctx)")
	g.P("}")
	g.P()
	g.P("// Cancel durably requests cancellation: the run stops selecting new")
	g.P("// forward work and unwinds successfully executed steps.")
	g.P("func (r ", name, "Run) Cancel(ctx ", ctx, ", cause string) error {")
	g.P("return r.run.Cancel(ctx, cause)")
	g.P("}")
	g.P()
	if pl.input != nil {
		g.P("// Input returns a defensive caller-owned copy of the run's immutable")
		g.P("// pipeline input. The input is released when the run reaches its")
		g.P("// terminal outcome; Input on a terminal run returns durable.ErrRunTerminal.")
		g.P("func (r ", name, "Run) Input(ctx ", ctx, ") (*", g.QualifiedGoIdent(pl.input.GoIdent), ", error) {")
		g.P("b, err := r.run.InputBytes(ctx)")
		g.P("if err != nil {")
		g.P("return nil, err")
		g.P("}")
		g.P("msg := &", g.QualifiedGoIdent(pl.input.GoIdent), "{}")
		g.P("if err := ", g.QualifiedGoIdent(protoPkg.Ident("Unmarshal")), "(b, msg); err != nil {")
		g.P("return nil, err")
		g.P("}")
		g.P("return msg, nil")
		g.P("}")
		g.P()
	}
	if !pl.typedResult() {
		g.P("// Wait blocks until the run is terminal.")
		g.P("func (r ", name, "Run) Wait(ctx ", ctx, ") (", g.QualifiedGoIdent(enginePkg.Ident("Result")), ", error) {")
		g.P("return r.run.Wait(ctx)")
		g.P("}")
		g.P()
		return
	}
	// decode emits the read of the terminal output bytes into a typed
	// message stored in the named Result field.
	decode := func(field string, msg *protogen.Message) {
		g.P("b, err := r.run.OutputBytes(ctx)")
		g.P("if err != nil {")
		g.P("return ", name, "Result{}, err")
		g.P("}")
		g.P("msg := &", g.QualifiedGoIdent(msg.GoIdent), "{}")
		g.P("if err := ", g.QualifiedGoIdent(protoPkg.Ident("Unmarshal")), "(b, msg); err != nil {")
		g.P("return ", name, "Result{}, err")
		g.P("}")
		g.P("out.", field, " = msg")
	}
	g.P("// Wait blocks until the run is terminal. A successful result carries the")
	g.P("// pipeline output and a failed one the failure output, each when the")
	g.P("// pipeline declares it.")
	g.P("func (r ", name, "Run) Wait(ctx ", ctx, ") (", name, "Result, error) {")
	g.P("res, err := r.run.Wait(ctx)")
	g.P("if err != nil {")
	g.P("return ", name, "Result{}, err")
	g.P("}")
	g.P("out := ", name, "Result{Result: res}")
	if pl.output != nil {
		g.P("if res.Succeeded() {")
		decode("output", pl.output)
		g.P("}")
	}
	if pl.failureOutput != nil {
		g.P("if res.Failed() {")
		decode("failureOutput", pl.failureOutput)
		g.P("}")
	}
	g.P("return out, nil")
	g.P("}")
	g.P()
	g.P("// ", name, "Result is the typed terminal result of a run.")
	g.P("type ", name, "Result struct {")
	g.P(g.QualifiedGoIdent(enginePkg.Ident("Result")))
	if pl.output != nil {
		g.P("output *", g.QualifiedGoIdent(pl.output.GoIdent))
	}
	if pl.failureOutput != nil {
		g.P("failureOutput *", g.QualifiedGoIdent(pl.failureOutput.GoIdent))
	}
	g.P("}")
	g.P()
	if pl.output != nil {
		g.P("// Output returns the pipeline output. It is non-nil exactly when the run")
		g.P("// succeeded.")
		g.P("func (r ", name, "Result) Output() *", g.QualifiedGoIdent(pl.output.GoIdent), " { return r.output }")
		g.P()
	}
	if pl.failureOutput != nil {
		g.P("// FailureOutput returns the pipeline failure output, the failure")
		g.P("// reducer's account of the failed run. It is non-nil exactly when the")
		g.P("// run failed.")
		g.P("func (r ", name, "Result) FailureOutput() *", g.QualifiedGoIdent(pl.failureOutput.GoIdent), " { return r.failureOutput }")
		g.P()
	}
}

func strconv(s string) string { return fmt.Sprintf("%q", s) }
