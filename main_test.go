package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var rewrite = flag.Bool("rewrite", false, "regenerate golden output files in testdata/")

// testOpts returns a default opts struct suitable for deterministic tests.
// Callers override individual fields as needed.
func testOpts() struct {
	window             time.Duration
	spikeThreshold     time.Duration
	goroutineThreshold int
	timeseries         bool
	byCreator          bool
	gc                 bool
	bursts             bool
	worst              int
	top                int
	topWaiters         bool
	keepDB             bool
	sql                bool
	verbose            bool
} {
	return struct {
		window             time.Duration
		spikeThreshold     time.Duration
		goroutineThreshold int
		timeseries         bool
		byCreator          bool
		gc                 bool
		bursts             bool
		worst              int
		top                int
		topWaiters         bool
		keepDB             bool
		sql                bool
		verbose            bool
	}{
		window:             100 * time.Millisecond,
		spikeThreshold:     1 * time.Millisecond,
		goroutineThreshold: 40, // fixed value for deterministic output
		top:                5,
	}
}

// TestGoldenOutput runs schedstat on each .bin trace file in testdata/ and
// compares the output against the corresponding .txt golden file. When invoked
// with -rewrite, the golden files are regenerated instead.
//
// To add a new test case, drop a Go execution trace into testdata/ with a .bin
// extension. Then run:
//
//	go test -run TestGoldenOutput -rewrite
//
// to generate the initial golden file. From that point on, the test will fail
// if the output changes, making regressions easy to spot.
func TestGoldenOutput(t *testing.T) {
	traces, err := filepath.Glob("testdata/*.bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(traces) == 0 {
		t.Fatal("no .bin trace files found in testdata/")
	}

	// Use fixed flags for deterministic output.
	opts = testOpts()

	for _, traceFile := range traces {
		name := strings.TrimSuffix(filepath.Base(traceFile), ".bin")
		goldenFile := filepath.Join("testdata", name+".txt")

		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := runAnalysis(traceFile, &buf); err != nil {
				t.Fatalf("runAnalysis(%s): %v", traceFile, err)
			}
			got := buf.String()

			if *rewrite {
				if err := os.WriteFile(goldenFile, []byte(got), 0644); err != nil {
					t.Fatalf("writing golden file: %v", err)
				}
				t.Logf("rewrote %s", goldenFile)
				return
			}

			want, err := os.ReadFile(goldenFile)
			if err != nil {
				t.Fatalf("reading golden file (run with -rewrite to generate): %v", err)
			}

			if got != string(want) {
				t.Errorf("output mismatch for %s (run with -rewrite to update)\n\n%s",
					traceFile, lineDiff(string(want), got))
			}
		})
	}
}

// TestGoroutineThreshold verifies that the goroutine threshold triggers
// appropriately at different levels.
func TestGoroutineThreshold(t *testing.T) {
	traceFile := "testdata/experiment-upsert1000-gateway.bin"
	if _, err := os.Stat(traceFile); os.IsNotExist(err) {
		t.Skip("test trace not available")
	}

	tests := []struct {
		name               string
		goroutineThreshold int
		wantGoroutineSpikes bool
	}{
		{"low_threshold_triggers", 80, true},
		{"high_threshold_no_trigger", 3000, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts = testOpts()
			opts.goroutineThreshold = tc.goroutineThreshold

			var buf bytes.Buffer
			if err := runAnalysis(traceFile, &buf); err != nil {
				t.Fatalf("runAnalysis: %v", err)
			}
			output := buf.String()

			hasGoroutineSpikes := strings.Contains(output, "Runnable Spikes")
			if hasGoroutineSpikes != tc.wantGoroutineSpikes {
				t.Errorf("goroutine spikes: got %v, want %v", hasGoroutineSpikes, tc.wantGoroutineSpikes)
			}
		})
	}
}

// lineDiff returns a simple line-by-line comparison showing the first few
// differences, to keep test output manageable.
func lineDiff(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")

	var buf strings.Builder
	const maxDiffs = 10
	diffs := 0

	max := len(wantLines)
	if len(gotLines) > max {
		max = len(gotLines)
	}

	for i := 0; i < max; i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			diffs++
			if diffs > maxDiffs {
				fmt.Fprintf(&buf, "... (more differences omitted)\n")
				break
			}
			fmt.Fprintf(&buf, "--- want line %d:\n  %s\n", i+1, w)
			fmt.Fprintf(&buf, "+++ got  line %d:\n  %s\n", i+1, g)
		}
	}

	if len(wantLines) != len(gotLines) {
		fmt.Fprintf(&buf, "(want %d lines, got %d lines)\n", len(wantLines), len(gotLines))
	}

	return buf.String()
}
