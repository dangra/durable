// Command perfcompare gates the performance regression suite: it parses
// two `go test -bench` outputs (base and head), takes the median of each
// metric across -count repetitions, and applies per-metric-class
// thresholds — tight for deterministic counters, loose for wall-clock
// figures. It prints a markdown table (suitable for a GitHub job summary)
// and exits non-zero when any gated metric regresses.
//
// Usage:
//
//	perfcompare base.txt head.txt   compare and gate
//	perfcompare -report head.txt    print a single-run table, no gating
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// class describes how a metric unit is judged.
type class struct {
	threshold   float64 // fractional regression allowed
	lowerIsBad  bool    // metric where a decrease is a regression
	informative bool    // never gates
}

func classify(unit string) class {
	switch unit {
	// Deterministic store counters: scheduling-independent, gate tightly.
	case "diskB/run", "diskB/attempt", "txwrites/run":
		return class{threshold: 0.05}
	// Allocation counters: near-deterministic, small timing wiggle.
	case "B/op", "allocs/op":
		return class{threshold: 0.10}
	// Wall clock: noisy on shared runners, gate loosely.
	case "ns/op", "p50-ms", "p99-ms", "start-ms":
		return class{threshold: 0.25}
	case "runs/sec", "unwinds/sec":
		return class{threshold: 0.25, lowerIsBad: true}
	default:
		return class{informative: true}
	}
}

// samples maps benchmark -> unit -> observed values across -count runs.
type samples map[string]map[string][]float64

func parse(path string) (samples, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := samples{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
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
	return out, sc.Err()
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

func main() {
	reportOnly := flag.Bool("report", false, "print a single-run table without gating")
	flag.Parse()

	if *reportOnly {
		if flag.NArg() != 1 {
			fmt.Fprintln(os.Stderr, "usage: perfcompare -report head.txt")
			os.Exit(2)
		}
		head, err := parse(flag.Arg(0))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Println("### Performance suite")
		fmt.Println()
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
		fmt.Fprintln(os.Stderr, "usage: perfcompare base.txt head.txt")
		os.Exit(2)
	}
	base, err := parse(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	head, err := parse(flag.Arg(1))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

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
			if o != 0 {
				delta = (h - o) / o
			}
			c := classify(u)
			verdict := "ok"
			switch {
			case c.informative:
				verdict = "info"
			case !c.lowerIsBad && delta > c.threshold,
				c.lowerIsBad && -delta > c.threshold:
				verdict = fmt.Sprintf("**REGRESSION** (>%g%%)", c.threshold*100)
				regressions++
			}
			fmt.Printf("| %s | %s | %.4g | %.4g | %+.1f%% | %s |\n", b, u, o, h, delta*100, verdict)
		}
	}
	if regressions > 0 {
		fmt.Printf("\n%d gated metric(s) regressed.\n", regressions)
		os.Exit(1)
	}
	fmt.Println("\nAll gated metrics within thresholds.")
}
