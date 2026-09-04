package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mustParse writes content as a benchmark-output file and parses it,
// failing the test on parse errors.
func mustParse(t *testing.T, content string) (samples, []string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bench.txt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	s, failures, err := parse(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return s, failures
}

func TestParse(t *testing.T) {
	s, failures := mustParse(t, `
goos: linux
BenchmarkAlpha-8   1  1000 ns/op  5.000 transitions/run  200 diskB/run
BenchmarkAlpha-8   1  1200 ns/op  5.000 transitions/run  180 diskB/run
BenchmarkBeta-4    1  9999 ns/op
BenchmarkBeta-4    1  8888 ns/op nonnumeric junk
not a benchmark line
PASS
ok  	example.com/pkg	1.0s
`)
	if len(failures) != 0 {
		t.Fatalf("failures = %v", failures)
	}
	if got := s["BenchmarkAlpha"]["ns/op"]; len(got) != 2 || got[0] != 1000 || got[1] != 1200 {
		t.Fatalf("Alpha ns/op = %v (GOMAXPROCS suffix must not split samples)", got)
	}
	if got := s["BenchmarkAlpha"]["diskB/run"]; len(got) != 2 {
		t.Fatalf("Alpha diskB/run = %v", got)
	}
	if got := s["BenchmarkBeta"]["ns/op"]; len(got) != 2 || got[0] != 9999 || got[1] != 8888 {
		t.Fatalf("Beta = %v (non-numeric trailing tokens must be skipped, not fatal)", got)
	}
}

func TestParseCollectsFailureEvidence(t *testing.T) {
	var b strings.Builder
	b.WriteString("--- FAIL: BenchmarkX (0.1s)\n")
	b.WriteString("    panic: boom\n")
	b.WriteString("FAIL\texample.com/pkg\t0.2s\n")
	for i := 0; i < 30; i++ {
		b.WriteString("FAIL extra\n")
	}
	_, failures := mustParse(t, b.String())
	if len(failures) != 20 {
		t.Fatalf("failures capped at %d, want 20", len(failures))
	}
	for _, want := range []string{"--- FAIL: BenchmarkX", "panic: boom", "FAIL\texample.com/pkg"} {
		found := false
		for _, f := range failures {
			if strings.HasPrefix(f, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("failure evidence missing %q in %v", want, failures)
		}
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		unit string
		want class
	}{
		{"transitions/run", class{threshold: 0.001, twoSided: true}},
		{"transitions/cycle", class{threshold: 0.001, twoSided: true}},
		{"diskB/run", class{threshold: 0.15, bestOf: true}},
		{"B/op", class{threshold: 0.10}},
		{"allocs/op", class{threshold: 0.10}},
		{"ns/op", class{threshold: 1.00, pairedThreshold: 1.00, bestOf: true, wall: true, hardThreshold: 4.00}},
		{"p99-ms", class{threshold: 1.00, pairedThreshold: 1.00, bestOf: true, wall: true, hardThreshold: 4.00}},
		{"runs/sec", class{threshold: 0.50, pairedThreshold: 0.50, lowerIsBad: true, bestOf: true, wall: true, hardThreshold: 0.80}},
		{"wake-max-ms", class{informative: true}},
		{"anything-else", class{informative: true}},
	}
	for _, tt := range tests {
		if got := classify(tt.unit); got != tt.want {
			t.Errorf("classify(%q) = %+v, want %+v", tt.unit, got, tt.want)
		}
	}
}

func TestEstimate(t *testing.T) {
	vs := []float64{30, 10, 20}
	if got := estimate(vs, class{}); got != 20 {
		t.Fatalf("median estimate = %v", got)
	}
	if got := estimate(vs, class{bestOf: true}); got != 10 {
		t.Fatalf("best-of min = %v", got)
	}
	if got := estimate(vs, class{bestOf: true, lowerIsBad: true}); got != 30 {
		t.Fatalf("best-of max = %v", got)
	}
}

func TestPairedDelta(t *testing.T) {
	if got := pairedDelta([]float64{110, 500, 90}, []float64{100, 100, 100}); got != 0.10000000000000009 && math.Abs(got-0.1) > 1e-9 {
		t.Fatalf("paired median delta = %v, want ~+10%% (the 5x slice is the outlier)", got)
	}
	// ratios are [+Inf, 0]; the two-sample median picks index 1 = +Inf,
	// so a zero-base slice cannot hide inside a paired comparison.
	if got := pairedDelta([]float64{1, 2}, []float64{0, 2}); !math.IsInf(got, 1) {
		t.Fatalf("zero-base slice delta = %v, want +Inf", got)
	}
	if got := pairedDelta([]float64{0}, []float64{0}); got != 0 {
		t.Fatalf("both-zero delta = %v", got)
	}
}

// gateOn runs gate over two synthetic outputs and returns (regressions,
// table).
func gateOn(t *testing.T, baseTxt, headTxt string, allowMissing bool) (int, string) {
	t.Helper()
	base, _ := mustParse(t, baseTxt)
	head, _ := mustParse(t, headTxt)
	var sb strings.Builder
	n := gate(&sb, base, head, allowMissing)
	return n, sb.String()
}

func TestGateVerdicts(t *testing.T) {
	base := `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  1000 diskB/run  400.0 runs/sec  50000 B/op
BenchmarkA-4  1  105 ns/op  15.00 transitions/run  1100 diskB/run  390.0 runs/sec  50000 B/op
BenchmarkA-4  1   99 ns/op  15.00 transitions/run  1000 diskB/run  405.0 runs/sec  50000 B/op
`
	tests := []struct {
		name        string
		head        string
		regressions int
		want        []string
	}{
		{
			name: "identical passes",
			head: base,
			want: []string{"All gated metrics within thresholds"},
		},
		{
			name: "transitions gate is two-sided: a decrease fails",
			head: `
BenchmarkA-4  1  100 ns/op  14.00 transitions/run  1000 diskB/run  400.0 runs/sec  50000 B/op
BenchmarkA-4  1  105 ns/op  14.00 transitions/run  1100 diskB/run  390.0 runs/sec  50000 B/op
BenchmarkA-4  1   99 ns/op  14.00 transitions/run  1000 diskB/run  405.0 runs/sec  50000 B/op
`,
			regressions: 1,
			want:        []string{"transitions/run | 15 | 14 | -6.7% | **REGRESSION** (>±0.1%)"},
		},
		{
			name: "wall clock is a 2x smoke alarm: +43%-class noise passes",
			head: `
BenchmarkA-4  1  143 ns/op  15.00 transitions/run  1000 diskB/run  280.0 runs/sec  50000 B/op
BenchmarkA-4  1  150 ns/op  15.00 transitions/run  1100 diskB/run  270.0 runs/sec  50000 B/op
BenchmarkA-4  1  141 ns/op  15.00 transitions/run  1000 diskB/run  283.0 runs/sec  50000 B/op
`,
			want: []string{"All gated metrics within thresholds"},
		},
		{
			name: "a 2.3x wall blowup with flat counters is waived as runner noise",
			head: `
BenchmarkA-4  1  230 ns/op  15.00 transitions/run  1000 diskB/run  174.0 runs/sec  50000 B/op
BenchmarkA-4  1  240 ns/op  15.00 transitions/run  1100 diskB/run  166.0 runs/sec  50000 B/op
BenchmarkA-4  1  228 ns/op  15.00 transitions/run  1000 diskB/run  175.0 runs/sec  50000 B/op
`,
			want: []string{
				"waived (wall clock, uncorroborated)",
				"2 wall-clock alarm(s) waived",
			},
		},
		{
			// B/op +6% corroborates (past half its 10% gate) without
			// itself gating: the wall alarms fire.
			name: "a corroborated 2.3x wall blowup fails time and throughput",
			head: `
BenchmarkA-4  1  230 ns/op  15.00 transitions/run  1000 diskB/run  174.0 runs/sec  53000 B/op
BenchmarkA-4  1  240 ns/op  15.00 transitions/run  1100 diskB/run  166.0 runs/sec  53000 B/op
BenchmarkA-4  1  228 ns/op  15.00 transitions/run  1000 diskB/run  175.0 runs/sec  53000 B/op
`,
			regressions: 2,
			want: []string{
				"ns/op", "**REGRESSION** (>100%)",
				"runs/sec", "**REGRESSION** (>50%)",
				"| B/op | 5e+04 | 5.3e+04 | +6.0% | ok |",
			},
		},
		{
			// diskB +8% (past half its 15% gate, judged on the best
			// sample) corroborates without itself gating.
			name: "diskB movement below its own gate still corroborates wall alarms",
			head: `
BenchmarkA-4  1  230 ns/op  15.00 transitions/run  1080 diskB/run  174.0 runs/sec  50000 B/op
BenchmarkA-4  1  240 ns/op  15.00 transitions/run  1090 diskB/run  166.0 runs/sec  50000 B/op
BenchmarkA-4  1  228 ns/op  15.00 transitions/run  1085 diskB/run  175.0 runs/sec  50000 B/op
`,
			regressions: 2,
			want: []string{
				"ns/op", "**REGRESSION** (>100%)",
				"| diskB/run | 1000 | 1080 | +8.0% | ok |",
			},
		},
		{
			name: "an uncorroborated blowup past the hard ceiling still gates",
			head: `
BenchmarkA-4  1  600 ns/op  15.00 transitions/run  1000 diskB/run  60.0 runs/sec  50000 B/op
BenchmarkA-4  1  610 ns/op  15.00 transitions/run  1100 diskB/run  58.0 runs/sec  50000 B/op
BenchmarkA-4  1  605 ns/op  15.00 transitions/run  1000 diskB/run  62.0 runs/sec  50000 B/op
`,
			regressions: 2,
			want: []string{
				"**REGRESSION** (>400%, uncorroborated blowup)",
				"**REGRESSION** (>80%, uncorroborated blowup)",
			},
		},
		{
			name: "diskB gates on the best sample: bimodal medians pass",
			head: `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  1100 diskB/run  400.0 runs/sec  50000 B/op
BenchmarkA-4  1  105 ns/op  15.00 transitions/run  1100 diskB/run  390.0 runs/sec  50000 B/op
BenchmarkA-4  1   99 ns/op  15.00 transitions/run  1050 diskB/run  405.0 runs/sec  50000 B/op
`,
			want: []string{"| diskB/run | 1000 | 1050 | +5.0% | ok |"},
		},
		{
			name: "a uniform diskB regression fails",
			head: `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  1200 diskB/run  400.0 runs/sec  50000 B/op
BenchmarkA-4  1  105 ns/op  15.00 transitions/run  1250 diskB/run  390.0 runs/sec  50000 B/op
BenchmarkA-4  1   99 ns/op  15.00 transitions/run  1210 diskB/run  405.0 runs/sec  50000 B/op
`,
			regressions: 1,
			want:        []string{"diskB/run | 1000 | 1200 | +20.0% | **REGRESSION** (>15%)"},
		},
		{
			name: "allocation gate at 10 percent on the median",
			head: `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  1000 diskB/run  400.0 runs/sec  60000 B/op
BenchmarkA-4  1  105 ns/op  15.00 transitions/run  1100 diskB/run  390.0 runs/sec  60000 B/op
BenchmarkA-4  1   99 ns/op  15.00 transitions/run  1000 diskB/run  405.0 runs/sec  60000 B/op
`,
			regressions: 1,
			want:        []string{"B/op | 5e+04 | 6e+04 | +20.0% | **REGRESSION** (>10%)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, out := gateOn(t, base, tt.head, false)
			if n != tt.regressions {
				t.Fatalf("regressions = %d, want %d\n%s", n, tt.regressions, out)
			}
			for _, w := range tt.want {
				if !strings.Contains(out, w) {
					t.Fatalf("output missing %q:\n%s", w, out)
				}
			}
		})
	}
}

func TestGateMissingCoverage(t *testing.T) {
	base := `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  9.0 wake-max-ms
BenchmarkB-4  1  100 ns/op
`
	t.Run("gated metric missing from head fails", func(t *testing.T) {
		n, out := gateOn(t, base, `
BenchmarkA-4  1  100 ns/op  9.0 wake-max-ms
BenchmarkB-4  1  100 ns/op
`, false)
		if n != 1 || !strings.Contains(out, "| transitions/run | 15 | — | — | **MISSING from head** |") {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
	t.Run("informative metric missing is info", func(t *testing.T) {
		n, out := gateOn(t, base, `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run
BenchmarkB-4  1  100 ns/op
`, false)
		if n != 0 || !strings.Contains(out, "| wake-max-ms | 9 | — | — | missing (info) |") {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
	t.Run("missing benchmark fails", func(t *testing.T) {
		n, out := gateOn(t, base, `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  9.0 wake-max-ms
`, false)
		if n != 1 || !strings.Contains(out, "| BenchmarkB | — | — | — | — | **MISSING from head** |") {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
	t.Run("allow-missing downgrades both to info", func(t *testing.T) {
		n, out := gateOn(t, base, `
BenchmarkA-4  1  100 ns/op  9.0 wake-max-ms
`, true)
		if n != 0 || strings.Count(out, "missing (info)") != 2 {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
	t.Run("new metric and new benchmark report as new", func(t *testing.T) {
		n, out := gateOn(t, base, `
BenchmarkA-4  1  100 ns/op  15.00 transitions/run  9.0 wake-max-ms  7.0 fresh-metric/run
BenchmarkB-4  1  100 ns/op
BenchmarkC-4  1  100 ns/op
`, false)
		if n != 0 || !strings.Contains(out, "| BenchmarkA | fresh-metric/run | — | 7 | — | new |") {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
	t.Run("zero base to nonzero head is an infinite regression", func(t *testing.T) {
		n, out := gateOn(t, `
BenchmarkZ-4  1  100 ns/op  0.000 transitions/run
`, `
BenchmarkZ-4  1  100 ns/op  3.000 transitions/run
`, false)
		if n != 1 || !strings.Contains(out, "+∞") {
			t.Fatalf("n=%d\n%s", n, out)
		}
	})
}

func TestPairedGatingRequiresEqualCounts(t *testing.T) {
	// runs/sec (lowerIsBad, paired at 50%): identical medians but a
	// paired ratio pattern only the per-slice view can see. B/op +6%
	// corroborates the wall alarm without gating itself.
	base := `
BenchmarkP-4  1  100 ns/op  100.0 runs/sec  50000 B/op
BenchmarkP-4  1  100 ns/op  100.0 runs/sec  50000 B/op
BenchmarkP-4  1  100 ns/op  100.0 runs/sec  50000 B/op
`
	head := `
BenchmarkP-4  1  100 ns/op  45.0 runs/sec  53000 B/op
BenchmarkP-4  1  100 ns/op  44.0 runs/sec  53000 B/op
BenchmarkP-4  1  100 ns/op  46.0 runs/sec  53000 B/op
`
	n, out := gateOn(t, base, head, false)
	if n != 1 || !strings.Contains(out, "runs/sec") || !strings.Contains(out, "REGRESSION") {
		t.Fatalf("paired throughput drop not gated: n=%d\n%s", n, out)
	}

	// Unequal counts fall back to best-of estimates: head's single fast
	// sample sits within the unpaired threshold.
	headOne := `
BenchmarkP-4  1  100 ns/op  60.0 runs/sec  53000 B/op
`
	n, out = gateOn(t, base, headOne, false)
	if n != 0 {
		t.Fatalf("fallback best-of should pass -40%% single sample at the 50%% alarm: n=%d\n%s", n, out)
	}
}

func TestWriteReport(t *testing.T) {
	head, failures := mustParse(t, `
--- FAIL: BenchmarkX (0.1s)
BenchmarkA-4  1  100 ns/op  15.00 transitions/run
`)
	var sb strings.Builder
	writeReport(&sb, head, failures)
	out := sb.String()
	for _, w := range []string{
		"### Performance suite",
		"> ⚠️ the run recorded failures; values below may be incomplete:",
		"> `--- FAIL: BenchmarkX (0.1s)`",
		"| BenchmarkA | transitions/run | 15 |",
	} {
		if !strings.Contains(out, w) {
			t.Fatalf("report missing %q:\n%s", w, out)
		}
	}
}

func TestParseCompleteRefusals(t *testing.T) {
	write := func(content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "in.txt")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	t.Run("recorded failure refuses", func(t *testing.T) {
		_, err := parseComplete(write("--- FAIL: BenchmarkX (0.1s)\nBenchmarkX-4 1 100 ns/op\n"))
		if err == nil || !strings.Contains(err.Error(), "refusing to gate") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("empty results refuse", func(t *testing.T) {
		_, err := parseComplete(write("goos: linux\nPASS\n"))
		if err == nil || !strings.Contains(err.Error(), "no benchmark results") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unreadable file errors", func(t *testing.T) {
		if _, err := parseComplete(filepath.Join(t.TempDir(), "absent.txt")); err == nil {
			t.Fatal("missing file did not error")
		}
	})
	t.Run("clean input parses", func(t *testing.T) {
		s, err := parseComplete(write("BenchmarkX-4 1 100 ns/op\nPASS\n"))
		if err != nil || len(s) != 1 {
			t.Fatalf("s=%v err=%v", s, err)
		}
	})
}

func TestParseScannerError(t *testing.T) {
	// A line beyond the 1MB scanner buffer surfaces as a parse error
	// rather than silent truncation.
	path := filepath.Join(t.TempDir(), "huge.txt")
	if err := os.WriteFile(path, append([]byte("BenchmarkHuge-4 1 "), make([]byte, 2<<20)...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := parse(path); err == nil {
		t.Fatal("oversized line did not error")
	}
}
