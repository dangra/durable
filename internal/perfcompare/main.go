// Command perfcompare gates the performance regression suite: it parses
// two `go test -bench` outputs (base and head), takes the median of each
// metric across -count repetitions, and applies per-metric-class
// thresholds — tight and two-sided for deterministic counters, loose for
// wall-clock figures. It prints a markdown table (suitable for a GitHub
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
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// class describes how a metric unit is judged.
type class struct {
	threshold   float64 // fractional regression allowed
	lowerIsBad  bool    // metric where a decrease is a regression
	twoSided    bool    // deterministic metric where any move is a regression
	informative bool    // never gates
}

func classify(unit string) class {
	switch unit {
	// Logical store-write counts: exactly deterministic per scenario —
	// any change in either direction is a real engine-behavior change
	// (a decrease can mean a lost durable write).
	case "transitions/run", "transitions/cycle":
		return class{threshold: 0.001, twoSided: true}
	// Physical disk bytes: near-deterministic, but the adaptive group
	// commit makes page accounting mildly timing-dependent.
	case "diskB/run", "diskB/attempt", "diskB/cycle":
		return class{threshold: 0.10}
	// Allocation counters: near-deterministic, small timing wiggle.
	case "B/op", "allocs/op":
		return class{threshold: 0.10}
	// Wall clock: noisy on shared runners, gate loosely.
	case "ns/op", "p50-ms", "p99-ms", "start-ms", "wake-p50-ms":
		return class{threshold: 0.25}
	case "runs/sec", "unwinds/sec", "cycles/sec":
		return class{threshold: 0.25, lowerIsBad: true}
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

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// mustParseComplete parses a gate-mode input and refuses to gate against
// untrustworthy data: recorded failures or an empty result set (a suite
// that panicked early emits no `--- FAIL:` line at all — absence of
// results is the only evidence left).
func mustParseComplete(path string) samples {
	s, failures, err := parse(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if len(failures) > 0 {
		fmt.Fprintf(os.Stderr, "%s recorded failures; refusing to gate:\n", path)
		for _, f := range failures {
			fmt.Fprintln(os.Stderr, "  "+f)
		}
		os.Exit(2)
	}
	if len(s) == 0 {
		fmt.Fprintf(os.Stderr, "no benchmark results in %s — the suite likely crashed before reporting\n", path)
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
		fmt.Println("### Performance suite")
		fmt.Println()
		// Report mode never gates, so a failure must not discard the
		// remaining valid metrics — surface it and keep going.
		if len(failures) > 0 {
			fmt.Println("> ⚠️ the run recorded failures; values below may be incomplete:")
			for _, f := range failures {
				fmt.Printf("> `%s`\n", f)
			}
			fmt.Println()
		}
		fmt.Println("| benchmark | metric | value |")
		fmt.Println("|---|---|---|")
		for _, b := range sortedKeys(head) {
			for _, u := range sortedKeys(head[b]) {
				fmt.Printf("| %s | %s | %.4g |\n", b, u, median(head[b][u]))
			}
		}
		return
	}

	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: perfcompare [-allow-missing] base.txt head.txt")
		os.Exit(2)
	}
	base := mustParseComplete(flag.Arg(0))
	head := mustParseComplete(flag.Arg(1))

	regressions := 0
	fmt.Println("### Performance comparison vs base")
	fmt.Println()
	fmt.Println("| benchmark | metric | base | head | delta | verdict |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, b := range sortedKeys(head) {
		for _, u := range sortedKeys(head[b]) {
			h := median(head[b][u])
			bs, ok := base[b][u]
			if !ok {
				fmt.Printf("| %s | %s | — | %.4g | — | new |\n", b, u, h)
				continue
			}
			o := median(bs)
			var delta float64
			switch {
			case o != 0:
				delta = (h - o) / o
			case h != 0:
				// 0 → nonzero must not slip through as delta 0.
				delta = math.Inf(1)
			}
			c := classify(u)
			verdict := "ok"
			switch {
			case c.informative:
				verdict = "info"
			case c.twoSided && math.Abs(delta) > c.threshold:
				verdict = fmt.Sprintf("**REGRESSION** (>±%g%%)", c.threshold*100)
				regressions++
			case !c.lowerIsBad && delta > c.threshold,
				c.lowerIsBad && -delta > c.threshold:
				verdict = fmt.Sprintf("**REGRESSION** (>%g%%)", c.threshold*100)
				regressions++
			}
			deltaStr := fmt.Sprintf("%+.1f%%", delta*100)
			if math.IsInf(delta, 1) {
				deltaStr = "+∞"
			}
			fmt.Printf("| %s | %s | %.4g | %.4g | %s | %s |\n", b, u, o, h, deltaStr, verdict)
		}
		// A gated metric that reported in base but not in head is lost
		// coverage: a deleted or renamed b.ReportMetric line must not
		// disarm its gate silently.
		for _, u := range sortedKeys(base[b]) {
			if _, ok := head[b][u]; ok {
				continue
			}
			if classify(u).informative || *allowMissing {
				fmt.Printf("| %s | %s | %.4g | — | — | missing (info) |\n", b, u, median(base[b][u]))
				continue
			}
			fmt.Printf("| %s | %s | %.4g | — | — | **MISSING from head** |\n", b, u, median(base[b][u]))
			regressions++
		}
	}
	// A benchmark that reported in base but vanished from head is lost
	// coverage, not a pass.
	for _, bname := range sortedKeys(base) {
		if _, ok := head[bname]; ok {
			continue
		}
		if *allowMissing {
			fmt.Printf("| %s | — | — | — | — | missing (info) |\n", bname)
			continue
		}
		fmt.Printf("| %s | — | — | — | — | **MISSING from head** |\n", bname)
		regressions++
	}
	if regressions > 0 {
		fmt.Printf("\n%d gated metric(s) regressed.\n", regressions)
		os.Exit(1)
	}
	fmt.Println("\nAll gated metrics within thresholds.")
}
