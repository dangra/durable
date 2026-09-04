// Command perfcompare gates the performance regression suite: it parses
// two `go test -bench` outputs (base and head) and applies
// per-metric-class thresholds: tight and two-sided for deterministic
// counters; best-sample estimates for disk-byte figures (runner
// interference only ever adds un-batched bytes); and for wall-clock
// figures the median per-slice head/base ratio when both sides carry
// the same sample count (CI interleaves the suites so paired slices
// share runner weather), falling back to loose best-sample estimates
// otherwise. It prints a markdown table (suitable for a GitHub
// job summary) and exits non-zero when any gated metric regresses, when
// gated coverage present in base is missing from head, or when either
// input recorded a test failure, panic, or build failure.
//
// Usage:
//
//	perfcompare base.txt head.txt                 compare and gate
//	perfcompare -allow-missing base.txt head.txt  tolerate focused -bench re-runs
//	perfcompare -report head.txt                  print a single-run table, no gating
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// class describes how a metric unit is judged.
type class struct {
	threshold   float64 // fractional regression allowed on the estimates
	lowerIsBad  bool    // metric where a decrease is a regression
	twoSided    bool    // deterministic metric where any move is a regression
	informative bool    // never gates
	bestOf      bool    // estimate with best-of-count samples, not the median
	// pairedThreshold, when nonzero and both sides carry the same number
	// of samples, gates on the median per-slice head/base ratio instead:
	// CI interleaves the suites so slice i of each side ran in adjacent
	// time windows and shared the runner weather, which the ratio
	// cancels — supporting a much tighter threshold than the unpaired
	// estimates.
	pairedThreshold float64
}

func classify(unit string) class {
	switch unit {
	// Logical store-write counts: exactly deterministic per scenario —
	// any change in either direction is a real engine-behavior change
	// (a decrease can mean a lost durable write).
	case "transitions/run", "transitions/cycle":
		return class{threshold: 0.001, twoSided: true}
	// Physical disk bytes: near-deterministic, but the adaptive group
	// commit makes page accounting timing-dependent one-sidedly —
	// scheduling noise only ever reduces coalescing and adds bytes — so
	// the smallest sample is the closest to the deterministic ideal.
	// 15%: best-sample floors of identical code have measured 11.8%
	// apart on the recovery scenario, whose coalescing modes are the
	// widest; real write-amplification regressions this gate exists for
	// measured 2x.
	case "diskB/run", "diskB/attempt", "diskB/cycle":
		return class{threshold: 0.15, bestOf: true}
	// Allocation counters: near-deterministic, small timing wiggle.
	case "B/op", "allocs/op":
		return class{threshold: 0.10}
	// Wall clock: a 2x smoke alarm, deliberately loose. Six gate
	// failures at tighter settings (25% and 50%, unpaired and paired,
	// interleaved and not) were all shared-runner noise — paired
	// medians of identical code have measured +43% — while every real
	// regression class this suite targets lands in the deterministic
	// counters above. Wall clock exists to catch order-of-magnitude
	// blowups with no counter movement (a hot pure-CPU loop), and 2x
	// does that while sitting above the measured noise ceiling.
	// Best-of/paired estimators are kept: interference only adds time,
	// and paired slices cancel shared weather.
	case "ns/op", "p50-ms", "p99-ms", "start-ms", "wake-p50-ms":
		return class{threshold: 1.00, pairedThreshold: 1.00, bestOf: true}
	case "runs/sec", "unwinds/sec", "cycles/sec":
		return class{threshold: 0.50, pairedThreshold: 0.50, lowerIsBad: true, bestOf: true}
	// wake-max-ms (a population max — one scheduler stall away from
	// doubling) and anything unrecognized: report, never gate.
	default:
		return class{informative: true}
	}
}

// samples maps benchmark -> unit -> observed values across -count runs.
type samples map[string]map[string][]float64

// parse reads one `go test -bench` output. Besides the metric samples it
// returns any failure evidence found: `--- FAIL:` benchmark lines, panic
// headers, and package-level `FAIL` lines (which is how a build failure
// or a mid-run crash surfaces). A file carrying any of those cannot be
// trusted as a complete set of results.
func parse(path string) (samples, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	out := samples{}
	var failures []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--- FAIL: ") ||
			strings.HasPrefix(trimmed, "panic: ") ||
			strings.HasPrefix(line, "FAIL") {
			if len(failures) < 20 {
				failures = append(failures, trimmed)
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 4 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		name := strings.SplitN(fields[0], "-", 2)[0] // strip -GOMAXPROCS
		for i := 2; i+1 < len(fields); i += 2 {
			v, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				continue
			}
			unit := fields[i+1]
			if out[name] == nil {
				out[name] = map[string][]float64{}
			}
			out[name][unit] = append(out[name][unit], v)
		}
	}
	return out, failures, sc.Err()
}

func median(vs []float64) float64 {
	s := append([]float64(nil), vs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

// estimate collapses one metric's samples per its class: classes with
// one-sided noise take the best sample (min, or max when lower is bad),
// all others the median.
func estimate(vs []float64, c class) float64 {
	if !c.bestOf {
		return median(vs)
	}
	best := vs[0]
	for _, v := range vs[1:] {
		if (v < best) != c.lowerIsBad {
			best = v
		}
	}
	return best
}

// pairedDelta is the median of per-slice head/base ratios, minus one.
// Slice i of each side ran in an adjacent time window under CI's
// interleaving, so the ratio cancels runner weather the slices shared.
func pairedDelta(hs, bs []float64) float64 {
	ratios := make([]float64, 0, len(hs))
	for i := range hs {
		switch {
		case bs[i] != 0:
			ratios = append(ratios, hs[i]/bs[i]-1)
		case hs[i] != 0:
			ratios = append(ratios, math.Inf(1))
		default:
			ratios = append(ratios, 0)
		}
	}
	return median(ratios)
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// parseComplete parses a gate-mode input and refuses to gate against
// untrustworthy data: recorded failures or an empty result set (a suite
// that panicked early emits no `--- FAIL:` line at all — absence of
// results is the only evidence left).
func parseComplete(path string) (samples, error) {
	s, failures, err := parse(path)
	if err != nil {
		return nil, err
	}
	if len(failures) > 0 {
		var b strings.Builder
		fmt.Fprintf(&b, "%s recorded failures; refusing to gate:", path)
		for _, f := range failures {
			b.WriteString("\n  " + f)
		}
		return nil, errors.New(b.String())
	}
	if len(s) == 0 {
		return nil, fmt.Errorf("no benchmark results in %s — the suite likely crashed before reporting", path)
	}
	return s, nil
}

// mustParseComplete is parseComplete with gate-mode exit semantics.
func mustParseComplete(path string) samples {
	s, err := parseComplete(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return s
}

func main() {
	reportOnly := flag.Bool("report", false, "print a single-run table without gating")
	allowMissing := flag.Bool("allow-missing", false,
		"report base-only benchmarks/metrics without gating them (for focused -bench re-runs)")
	flag.Parse()

	if *reportOnly {
		if flag.NArg() != 1 {
			fmt.Fprintln(os.Stderr, "usage: perfcompare -report head.txt")
			os.Exit(2)
		}
		head, failures, err := parse(flag.Arg(0))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		writeReport(os.Stdout, head, failures)
		return
	}

	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: perfcompare [-allow-missing] base.txt head.txt")
		os.Exit(2)
	}
	base := mustParseComplete(flag.Arg(0))
	head := mustParseComplete(flag.Arg(1))
	if gate(os.Stdout, base, head, *allowMissing) > 0 {
		os.Exit(1)
	}
}

// writeReport prints the ungated single-run table. Report mode never
// gates, so a recorded failure must not discard the remaining valid
// metrics — it is surfaced as a warning and the table follows.
func writeReport(w io.Writer, head samples, failures []string) {
	fmt.Fprintln(w, "### Performance suite")
	fmt.Fprintln(w)
	if len(failures) > 0 {
		fmt.Fprintln(w, "> ⚠️ the run recorded failures; values below may be incomplete:")
		for _, f := range failures {
			fmt.Fprintf(w, "> `%s`\n", f)
		}
		fmt.Fprintln(w)
	}
	fmt.Fprintln(w, "| benchmark | metric | value |")
	fmt.Fprintln(w, "|---|---|---|")
	for _, b := range sortedKeys(head) {
		for _, u := range sortedKeys(head[b]) {
			fmt.Fprintf(w, "| %s | %s | %.4g |\n", b, u, median(head[b][u]))
		}
	}
}

// gate prints the comparison table and returns the number of gated
// regressions, missing gated coverage included.
func gate(w io.Writer, base, head samples, allowMissing bool) int {
	regressions := 0
	fmt.Fprintln(w, "### Performance comparison vs base")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| benchmark | metric | base | head | delta | verdict |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|")
	for _, b := range sortedKeys(head) {
		for _, u := range sortedKeys(head[b]) {
			c := classify(u)
			hs := head[b][u]
			h := estimate(hs, c)
			bs, ok := base[b][u]
			if !ok {
				fmt.Fprintf(w, "| %s | %s | — | %.4g | — | new |\n", b, u, h)
				continue
			}
			o := estimate(bs, c)
			var delta float64
			switch {
			case o != 0:
				delta = (h - o) / o
			case h != 0:
				// 0 → nonzero must not slip through as delta 0.
				delta = math.Inf(1)
			}
			threshold := c.threshold
			if c.pairedThreshold > 0 && len(hs) == len(bs) && len(hs) > 1 {
				delta, threshold = pairedDelta(hs, bs), c.pairedThreshold
			}
			verdict := "ok"
			switch {
			case c.informative:
				verdict = "info"
			case c.twoSided && math.Abs(delta) > threshold:
				verdict = fmt.Sprintf("**REGRESSION** (>±%g%%)", threshold*100)
				regressions++
			case !c.lowerIsBad && delta > threshold,
				c.lowerIsBad && -delta > threshold:
				verdict = fmt.Sprintf("**REGRESSION** (>%g%%)", threshold*100)
				regressions++
			}
			deltaStr := fmt.Sprintf("%+.1f%%", delta*100)
			if math.IsInf(delta, 1) {
				deltaStr = "+∞"
			}
			fmt.Fprintf(w, "| %s | %s | %.4g | %.4g | %s | %s |\n", b, u, o, h, deltaStr, verdict)
		}
		// A gated metric that reported in base but not in head is lost
		// coverage: a deleted or renamed b.ReportMetric line must not
		// disarm its gate silently.
		for _, u := range sortedKeys(base[b]) {
			if _, ok := head[b][u]; ok {
				continue
			}
			if classify(u).informative || allowMissing {
				fmt.Fprintf(w, "| %s | %s | %.4g | — | — | missing (info) |\n", b, u, median(base[b][u]))
				continue
			}
			fmt.Fprintf(w, "| %s | %s | %.4g | — | — | **MISSING from head** |\n", b, u, median(base[b][u]))
			regressions++
		}
	}
	// A benchmark that reported in base but vanished from head is lost
	// coverage, not a pass.
	for _, bname := range sortedKeys(base) {
		if _, ok := head[bname]; ok {
			continue
		}
		if allowMissing {
			fmt.Fprintf(w, "| %s | — | — | — | — | missing (info) |\n", bname)
			continue
		}
		fmt.Fprintf(w, "| %s | — | — | — | — | **MISSING from head** |\n", bname)
		regressions++
	}
	if regressions > 0 {
		fmt.Fprintf(w, "\n%d gated metric(s) regressed.\n", regressions)
	} else {
		fmt.Fprintln(w, "\nAll gated metrics within thresholds.")
	}
	return regressions
}
