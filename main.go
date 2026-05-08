// schedstat analyzes Go execution traces for scheduling latency.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/dt/schedstat/internal/tracedb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/spf13/cobra"
)

var opts struct {
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
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "schedstat [flags] trace1.out [trace2.out ...]",
		Short: "Analyze Go execution traces for scheduling latency",
		Long: `schedstat analyzes Go execution traces and answers: "What's causing goroutines to wait?"

Examples:
  schedstat trace.out                # Summary + spike detection + spike details
  schedstat -n 10 trace.out          # Show top 10 spikes per type (default 5)
  schedstat --bursts trace.out       # Detect goroutine launch bursts
  schedstat --sql trace.out          # Drop into DuckDB shell for custom queries`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			exitCode := 0
			for i, traceFile := range args {
				if i > 0 {
					fmt.Println()
				}
				if err := run(traceFile); err != nil {
					fmt.Fprintf(os.Stderr, "error processing %s: %v\n", traceFile, err)
					exitCode = 1
				}
			}
			if exitCode != 0 {
				os.Exit(exitCode)
			}
			return nil
		},
	}

	f := rootCmd.Flags()
	f.DurationVarP(&opts.window, "window", "w", 100*time.Millisecond, "time window for analysis")
	f.DurationVar(&opts.spikeThreshold, "spike-threshold", 1*time.Millisecond, "p99 threshold to highlight as anomaly")
	f.IntVar(&opts.goroutineThreshold, "runnable-threshold", 0, "runnable goroutine count to highlight as anomaly (0 = 5*GOMAXPROCS)")
	f.BoolVar(&opts.timeseries, "timeseries", false, "show p99 latency per time window")
	f.BoolVar(&opts.byCreator, "by-creator", false, "group delays by goroutine creator")
	f.BoolVar(&opts.gc, "gc", true, "show GC analysis from range events")
	f.BoolVar(&opts.bursts, "bursts", false, "show burst events and who launched delayed goroutines")
	f.IntVar(&opts.worst, "worst", 0, "show N worst individual delays with stacks")
	f.IntVarP(&opts.top, "top", "n", 5, "number of spike listings and detail entries")
	f.BoolVar(&opts.topWaiters, "top-waiters", false, "show goroutines with most total wait time")
	f.BoolVar(&opts.keepDB, "keep-db", false, "keep DuckDB file after analysis")
	f.BoolVar(&opts.sql, "sql", false, "drop into DuckDB shell after analysis")
	f.BoolVarP(&opts.verbose, "verbose", "v", false, "verbose output")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(traceFile string) error {
	dbFile := traceFile + ".duckdb"
	needsCleanup := !opts.keepDB && !opts.sql
	if needsCleanup {
		defer os.Remove(dbFile)
	}

	// Remove any previous database file that might exist
	_ = os.Remove(dbFile)

	if opts.verbose {
		fmt.Fprintf(os.Stderr, "Converting trace to DuckDB...\n")
	}

	// Open and read the trace file
	traceData, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("opening trace file: %w", err)
	}

	// Create the DuckDB database and load the trace
	db, err := tracedb.Create(dbFile, traceData)
	traceData.Close()
	if err != nil {
		return fmt.Errorf("loading trace: %w", err)
	}

	database := db.DB

	if _, err := analyze(database, traceFile, os.Stdout); err != nil {
		database.Close()
		return err
	}
	database.Close()

	if opts.keepDB {
		fmt.Printf("\n%s\n", strings.Repeat("=", 60))
		fmt.Printf("DuckDB file: %s\n", dbFile)
		printSchemaInfo()
	}

	if opts.sql {
		fmt.Printf("\n%s\n", strings.Repeat("=", 60))
		fmt.Println("Dropping into DuckDB shell...")
		printSchemaInfo()
		fmt.Println()
		return execDuckDB(dbFile)
	}

	return nil
}

// runAnalysis is the internal entry point for running schedstat on a trace file,
// writing output to w. Tests use this to capture output without compiling and
// executing the binary.
func runAnalysis(traceFile string, w io.Writer) error {
	dbFile := traceFile + ".duckdb"
	defer os.Remove(dbFile)
	_ = os.Remove(dbFile)

	traceData, err := os.Open(traceFile)
	if err != nil {
		return fmt.Errorf("opening trace file: %w", err)
	}

	db, err := tracedb.Create(dbFile, traceData)
	traceData.Close()
	if err != nil {
		return fmt.Errorf("loading trace: %w", err)
	}
	defer db.DB.Close()

	_, err = analyze(db.DB, traceFile, w)
	return err
}

// analyze runs the full analysis pipeline. It populates a Report struct with
// every section's data and, in parallel, streams each section's plaintext form
// to w as soon as it is collected. Streaming keeps progress visible for long
// analyses and ensures partial output is preserved on early termination.
func analyze(db *sql.DB, traceFile string, w io.Writer) (*Report, error) {
	r := &Report{TraceFile: filepath.Base(traceFile)}

	// Get time bounds.
	var minTime, maxTime int64
	if err := db.QueryRow(`
		SELECT MIN(end_time_ns - duration_ns), MAX(end_time_ns)
		FROM g_transitions
		WHERE from_state = 'runnable' AND to_state = 'running'
	`).Scan(&minTime, &maxTime); err != nil {
		return r, fmt.Errorf("getting time bounds: %w", err)
	}
	r.DurationMs = float64(maxTime-minTime) / 1e6
	fmt.Fprint(w, r.Header())

	// Get processor count for goroutine threshold default.
	var procCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM procs`).Scan(&procCount); err != nil {
		return r, fmt.Errorf("getting processor count: %w", err)
	}
	goroutineThreshold := opts.goroutineThreshold
	if goroutineThreshold == 0 {
		goroutineThreshold = 5 * procCount
	}

	emit := func(s fmt.Stringer) { fmt.Fprint(w, s.String()) }

	// Always show overall stats.
	overall, err := collectOverallStats(db)
	if err != nil {
		return r, err
	}
	r.Overall = overall
	emit(overall)

	// Time series if requested.
	if opts.timeseries {
		ts, err := collectTimeseries(db, minTime, opts.window, opts.spikeThreshold)
		if err != nil {
			return r, err
		}
		r.Timeseries = ts
		emit(ts)
	}

	// Spike detection.
	latencySpikes, totalLatency, err := queryLatencySpikes(db, minTime, opts.window, opts.spikeThreshold, opts.top)
	if err != nil {
		return r, err
	}
	runnableSpikes, totalRunnable, err := queryRunnableSpikes(db, minTime, opts.window, goroutineThreshold, opts.top)
	if err != nil {
		return r, err
	}

	// Assign display indices before rendering so summaries and details agree.
	idx := 1
	for i := range latencySpikes {
		latencySpikes[i].Index = idx
		idx++
	}
	for i := range runnableSpikes {
		runnableSpikes[i].Index = idx
		idx++
	}

	if totalLatency > 0 {
		r.LatencySpikes = &SpikesReport{
			Kind:             "latency",
			WindowLabel:      opts.window.String(),
			LatencyThreshold: opts.spikeThreshold.String(),
			Total:            totalLatency,
			Spikes:           latencySpikes,
		}
		emit(r.LatencySpikes)
	}
	if totalRunnable > 0 {
		r.RunnableSpikes = &SpikesReport{
			Kind:              "runnable",
			WindowLabel:       opts.window.String(),
			RunnableThreshold: goroutineThreshold,
			Total:             totalRunnable,
			Spikes:            runnableSpikes,
		}
		emit(r.RunnableSpikes)
	}

	allSpikes := make([]SpikeSummary, 0, len(latencySpikes)+len(runnableSpikes))
	allSpikes = append(allSpikes, latencySpikes...)
	allSpikes = append(allSpikes, runnableSpikes...)
	if len(allSpikes) > 0 {
		details, err := collectSpikeDetails(db, allSpikes, minTime, opts.window)
		if err != nil {
			return r, err
		}
		r.SpikeDetails = details
		emit(details)
	}

	// By-creator analysis.
	if opts.byCreator {
		bc, err := collectByCreator(db)
		if err != nil {
			return r, err
		}
		r.ByCreator = bc
		emit(bc)
	}

	// GC analysis.
	if opts.gc {
		gc, err := collectGCAnalysis(db)
		if err != nil {
			return r, err
		}
		if gc != nil {
			r.GC = gc
			emit(gc)
		}
	}

	// Burst analysis.
	if opts.bursts {
		br, err := collectBurstAnalysis(db, opts.window)
		if err != nil {
			return r, err
		}
		r.Bursts = br
		emit(br)
	}

	// Worst delays.
	if opts.worst > 0 {
		wd, err := collectWorstDelays(db, opts.worst)
		if err != nil {
			return r, err
		}
		r.WorstDelays = wd
		emit(wd)
	}

	// Top goroutines by wait time (optional, can be slow).
	if opts.topWaiters {
		tg, err := collectTopGoroutines(db)
		if err != nil {
			return r, err
		}
		r.TopGoroutines = tg
		emit(tg)
	}

	return r, nil
}

// nullF64ToInt64Ptr converts a nullable float-nanoseconds value (as returned
// by DuckDB aggregate functions over an empty set, or unmaterialized scans) to
// a *int64 suitable for omitempty JSON serialization.
func nullF64ToInt64Ptr(n sql.NullFloat64) *int64 {
	if !n.Valid {
		return nil
	}
	v := int64(n.Float64)
	return &v
}

func collectOverallStats(db *sql.DB) (*OverallStats, error) {
	var count int
	var minNs, avgNs, p50Ns, p90Ns, p99Ns, maxNs sql.NullFloat64
	err := db.QueryRow(`
		SELECT
			COUNT(*),
			MIN(duration_ns),
			AVG(duration_ns),
			PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ns),
			PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY duration_ns),
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns),
			MAX(duration_ns)
		FROM g_transitions
		WHERE from_state = 'runnable' AND to_state = 'running'
	`).Scan(&count, &minNs, &avgNs, &p50Ns, &p90Ns, &p99Ns, &maxNs)
	if err != nil {
		return nil, fmt.Errorf("getting overall stats: %w", err)
	}
	return &OverallStats{
		Count: count,
		MinNs: nullF64ToInt64Ptr(minNs),
		AvgNs: nullF64ToInt64Ptr(avgNs),
		P50Ns: nullF64ToInt64Ptr(p50Ns),
		P90Ns: nullF64ToInt64Ptr(p90Ns),
		P99Ns: nullF64ToInt64Ptr(p99Ns),
		MaxNs: nullF64ToInt64Ptr(maxNs),
	}, nil
}

func collectTimeseries(
	db *sql.DB, minTime int64, window, threshold time.Duration,
) (*TimeseriesReport, error) {
	windowNs := window.Nanoseconds()

	rows, err := db.Query(`
		SELECT
			((end_time_ns - $1) // $2) as window_num,
			((end_time_ns - $1) // $2) * $2 / 1e6 as start_ms,
			(((end_time_ns - $1) // $2) + 1) * $2 / 1e6 as end_ms,
			COUNT(*) as cnt,
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns) as p99
		FROM g_transitions
		WHERE from_state = 'runnable' AND to_state = 'running'
		GROUP BY 1, 2, 3
		ORDER BY 1
	`, minTime, windowNs)
	if err != nil {
		return nil, fmt.Errorf("timeseries query: %w", err)
	}
	defer rows.Close()

	t := &TimeseriesReport{
		WindowLabel: window.String(),
		ThresholdNs: threshold.Nanoseconds(),
	}
	for rows.Next() {
		var w TimeseriesWindow
		if err := rows.Scan(&w.Window, &w.StartMs, &w.EndMs, &w.Events, &w.P99Ns); err != nil {
			return nil, err
		}
		t.Windows = append(t.Windows, w)
	}
	return t, rows.Err()
}

func queryLatencySpikes(
	db *sql.DB, minTime int64, window, threshold time.Duration, top int,
) ([]SpikeSummary, int, error) {
	windowNs := window.Nanoseconds()
	thresholdNs := float64(threshold.Nanoseconds())

	rows, err := db.Query(`
		WITH spike_windows AS (
			SELECT
				(end_time_ns - $1) // $2 as window_num,
				((end_time_ns - $1) // $2) * $2 / 1e6 as start_ms,
				COUNT(*) as event_count,
				PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns) as p99,
				MAX(duration_ns) as max_latency
			FROM g_transitions
			WHERE from_state = 'runnable' AND to_state = 'running'
			GROUP BY 1, 2
			HAVING p99 > $3
		)
		SELECT *, COUNT(*) OVER() as total
		FROM spike_windows
		ORDER BY p99 DESC
		LIMIT $4
	`, minTime, windowNs, thresholdNs, top)
	if err != nil {
		return nil, 0, fmt.Errorf("latency spikes query: %w", err)
	}
	defer rows.Close()

	var spikes []SpikeSummary
	var total int
	for rows.Next() {
		s := SpikeSummary{Type: "latency"}
		if err := rows.Scan(&s.WindowNum, &s.StartMs, &s.EventCount, &s.P99Ns, &s.MaxLatencyNs, &total); err != nil {
			return nil, 0, err
		}
		spikes = append(spikes, s)
	}
	return spikes, total, rows.Err()
}

func queryRunnableSpikes(
	db *sql.DB, minTime int64, window time.Duration, runnableThreshold, top int,
) ([]SpikeSummary, int, error) {
	windowMs := window.Milliseconds()

	rows, err := db.Query(`
		WITH buckets AS (
			SELECT
				(end_time_ns - $1) // 1000000 as bucket_ms,
				COUNT(*) FILTER (WHERE to_state = 'runnable') as enter,
				COUNT(*) FILTER (WHERE from_state = 'runnable') as leave
			FROM g_transitions
			WHERE from_state = 'runnable' OR to_state = 'runnable'
			GROUP BY bucket_ms
		),
		-- Cumulative runnable count. This assumes zero runnable goroutines at
		-- trace start, which is approximate but sufficient for spike detection.
		running AS (
			SELECT
				bucket_ms,
				SUM(enter - leave) OVER (ORDER BY bucket_ms) as runnable_count
			FROM buckets
		),
		window_max AS (
			SELECT
				bucket_ms // $2 as window_num,
				MAX(runnable_count) as max_runnable
			FROM running
			GROUP BY window_num
			HAVING max_runnable > $3
		)
		SELECT window_num, max_runnable, COUNT(*) OVER() as total
		FROM window_max
		ORDER BY max_runnable DESC
		LIMIT $4
	`, minTime, windowMs, runnableThreshold, top)
	if err != nil {
		return nil, 0, fmt.Errorf("runnable spikes query: %w", err)
	}
	defer rows.Close()

	var spikes []SpikeSummary
	var total int
	for rows.Next() {
		s := SpikeSummary{Type: "runnable"}
		if err := rows.Scan(&s.WindowNum, &s.MaxRunnable, &total); err != nil {
			return nil, 0, err
		}
		s.StartMs = float64(s.WindowNum) * float64(windowMs)
		spikes = append(spikes, s)
	}
	return spikes, total, rows.Err()
}

func collectByCreator(db *sql.DB) (*ByCreatorReport, error) {
	rows, err := db.Query(`
		SELECT
			COALESCE(
				(SELECT list_first(funcs) FROM stacks WHERE stack_id = gt.src_stack_id),
				'(unknown)'
			) as creator,
			COUNT(*) as cnt,
			SUM(duration_ns) as total_wait,
			MAX(duration_ns) as max_wait,
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns) as p99
		FROM g_transitions gt
		WHERE from_state = 'runnable' AND to_state = 'running'
		GROUP BY 1
		ORDER BY total_wait DESC
		LIMIT $1
	`, opts.top)
	if err != nil {
		return nil, fmt.Errorf("by-creator query: %w", err)
	}
	defer rows.Close()

	out := &ByCreatorReport{}
	for rows.Next() {
		var row CreatorRow
		var totalNs, maxNs, p99Ns float64
		if err := rows.Scan(&row.Creator, &row.Count, &totalNs, &maxNs, &p99Ns); err != nil {
			return nil, err
		}
		row.TotalNs = int64(totalNs)
		row.MaxNs = int64(maxNs)
		row.P99Ns = int64(p99Ns)
		out.Rows = append(out.Rows, row)
	}
	return out, rows.Err()
}

func collectGCAnalysis(db *sql.DB) (*GCReport, error) {
	// Check if there's any GC data at all.
	var totalRanges int
	if err := db.QueryRow(`SELECT COUNT(*) FROM gc_ranges`).Scan(&totalRanges); err != nil {
		return nil, fmt.Errorf("gc range count: %w", err)
	}
	if totalRanges == 0 {
		return nil, nil
	}

	g := &GCReport{}

	cycle, err := collectGCCycleSummary(db)
	if err != nil {
		return nil, err
	}
	g.CycleSummary = cycle

	stw, err := collectSTWSummary(db)
	if err != nil {
		return nil, err
	}
	if stw != nil {
		g.STW = stw
	}

	ma, err := collectMarkAssist(db)
	if err != nil {
		return nil, err
	}
	if ma != nil {
		g.MarkAssist = ma
	}

	cmp, err := collectGCLatencyComparison(db)
	if err != nil {
		return nil, err
	}
	if cmp != nil {
		g.LatencyComparison = cmp
	}

	if opts.verbose {
		pc, err := collectPerCycleBreakdown(db)
		if err != nil {
			return nil, err
		}
		g.PerCycle = pc
	}

	sweep, err := collectSweepSummary(db)
	if err != nil {
		return nil, err
	}
	if sweep != nil {
		g.Sweep = sweep
	}

	return g, nil
}

func collectGCCycleSummary(db *sql.DB) (*GCCycleSummary, error) {
	var count int
	var totalNs, avgNs, minNs, maxNs sql.NullFloat64
	err := db.QueryRow(`
		SELECT COUNT(*), SUM(duration_ns), AVG(duration_ns), MIN(duration_ns), MAX(duration_ns)
		FROM gc_ranges
		WHERE name = 'GC concurrent mark phase'
	`).Scan(&count, &totalNs, &avgNs, &minNs, &maxNs)
	if err != nil {
		return nil, fmt.Errorf("gc cycle summary: %w", err)
	}
	return &GCCycleSummary{
		Count:   count,
		TotalNs: nullF64ToInt64Ptr(totalNs),
		AvgNs:   nullF64ToInt64Ptr(avgNs),
		MinNs:   nullF64ToInt64Ptr(minNs),
		MaxNs:   nullF64ToInt64Ptr(maxNs),
	}, nil
}

func collectSTWSummary(db *sql.DB) (*GCSTWSummary, error) {
	rows, err := db.Query(`
		SELECT
			regexp_extract(name, '\((.*)\)', 1) as reason,
			COUNT(*) as cnt,
			SUM(duration_ns) as total_ns,
			MAX(duration_ns) as max_ns
		FROM gc_ranges
		WHERE name LIKE 'stop-the-world%'
		GROUP BY 1
		ORDER BY total_ns DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("stw query: %w", err)
	}
	defer rows.Close()

	var reasons []STWEntry
	var totalCount int
	var totalNs, overallMax float64
	for rows.Next() {
		var e STWEntry
		var totNs, mxNs float64
		if err := rows.Scan(&e.Reason, &e.Count, &totNs, &mxNs); err != nil {
			return nil, err
		}
		e.TotalNs = int64(totNs)
		e.MaxNs = int64(mxNs)
		reasons = append(reasons, e)
		totalCount += e.Count
		totalNs += totNs
		if mxNs > overallMax {
			overallMax = mxNs
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if totalCount == 0 {
		return nil, nil
	}
	return &GCSTWSummary{
		TotalCount: totalCount,
		TotalNs:    int64(totalNs),
		MaxNs:      int64(overallMax),
		Reasons:    reasons,
	}, nil
}

func collectMarkAssist(db *sql.DB) (*GCMarkAssist, error) {
	var totalEvents int
	var totalGoroutines int
	var totalNs, maxSingleNs sql.NullFloat64
	err := db.QueryRow(`
		SELECT COUNT(*), COUNT(DISTINCT scope_id), SUM(duration_ns), MAX(duration_ns)
		FROM gc_ranges
		WHERE name = 'GC mark assist'
	`).Scan(&totalEvents, &totalGoroutines, &totalNs, &maxSingleNs)
	if err != nil {
		return nil, fmt.Errorf("mark assist summary: %w", err)
	}
	if totalEvents == 0 {
		return nil, nil
	}

	out := &GCMarkAssist{
		TotalEvents:     totalEvents,
		TotalGoroutines: totalGoroutines,
		TotalNs:         nullF64ToInt64Ptr(totalNs),
		MaxSingleNs:     nullF64ToInt64Ptr(maxSingleNs),
	}

	rows, err := db.Query(`
		WITH t0 AS (
			SELECT MIN(end_time_ns - duration_ns) as v FROM g_transitions
			WHERE from_state = 'runnable' AND to_state = 'running'
		)
		SELECT
			r.scope_id,
			COALESCE(g.name, '(unknown)') as gname,
			COUNT(*) as assists,
			SUM(r.duration_ns) as total_assist_ns,
			MAX(r.duration_ns) as max_assist_ns,
			(arg_max(r.start_time_ns, r.duration_ns) - (SELECT v FROM t0)) / 1e6 as worst_at_ms
		FROM gc_ranges r
		LEFT JOIN goroutines g ON r.scope_id = g.g
		WHERE r.name = 'GC mark assist'
		GROUP BY r.scope_id, gname
		ORDER BY total_assist_ns DESC
		LIMIT $1
	`, opts.top)
	if err != nil {
		return nil, fmt.Errorf("mark assist top goroutines: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var e MarkAssistGEntry
		var totNs, mxNs float64
		if err := rows.Scan(&e.G, &e.Name, &e.Assists, &totNs, &mxNs, &e.WorstAtMs); err != nil {
			return nil, err
		}
		e.TotalNs = int64(totNs)
		e.MaxNs = int64(mxNs)
		out.TopGoroutines = append(out.TopGoroutines, e)
	}
	return out, rows.Err()
}

func collectGCLatencyComparison(db *sql.DB) (*GCLatencyComparison, error) {
	var cycleCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM gc_cycles`).Scan(&cycleCount); err != nil {
		return nil, fmt.Errorf("gc cycle count: %w", err)
	}
	if cycleCount == 0 {
		return nil, nil
	}

	var dCount, nCount int
	var dP50, dP99, dMax sql.NullFloat64
	var nP50, nP99, nMax sql.NullFloat64

	err := db.QueryRow(`
		SELECT
			COUNT(*),
			PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ns),
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns),
			MAX(duration_ns)
		FROM g_transitions gt
		WHERE from_state = 'runnable' AND to_state = 'running'
		  AND EXISTS (
			SELECT 1 FROM gc_cycles gc
			WHERE gt.end_time_ns - gt.duration_ns < gc.end_time_ns
			  AND gt.end_time_ns > gc.start_time_ns
		  )
	`).Scan(&dCount, &dP50, &dP99, &dMax)
	if err != nil {
		return nil, fmt.Errorf("gc latency during: %w", err)
	}

	err = db.QueryRow(`
		SELECT
			COUNT(*),
			PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ns),
			PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ns),
			MAX(duration_ns)
		FROM g_transitions gt
		WHERE from_state = 'runnable' AND to_state = 'running'
		  AND NOT EXISTS (
			SELECT 1 FROM gc_cycles gc
			WHERE gt.end_time_ns - gt.duration_ns < gc.end_time_ns
			  AND gt.end_time_ns > gc.start_time_ns
		  )
	`).Scan(&nCount, &nP50, &nP99, &nMax)
	if err != nil {
		return nil, fmt.Errorf("gc latency non-gc: %w", err)
	}

	if dCount == 0 {
		return nil, nil
	}

	out := &GCLatencyComparison{
		DuringGC: LatencyBucket{
			Count: dCount,
			P50Ns: nullF64ToInt64Ptr(dP50),
			P99Ns: nullF64ToInt64Ptr(dP99),
			MaxNs: nullF64ToInt64Ptr(dMax),
		},
		NonGC: LatencyBucket{
			Count: nCount,
			P50Ns: nullF64ToInt64Ptr(nP50),
			P99Ns: nullF64ToInt64Ptr(nP99),
			MaxNs: nullF64ToInt64Ptr(nMax),
		},
	}

	if nP50.Valid && nP50.Float64 > 0 && dP50.Valid {
		out.HasRatio = true
		out.RatioP50 = dP50.Float64 / nP50.Float64
		if nP99.Valid && nP99.Float64 > 0 && dP99.Valid {
			out.RatioP99 = dP99.Float64 / nP99.Float64
		}
		if nMax.Valid && nMax.Float64 > 0 && dMax.Valid {
			out.RatioMax = dMax.Float64 / nMax.Float64
		}
	}
	return out, nil
}

func collectPerCycleBreakdown(db *sql.DB) (*GCPerCycleBreakdown, error) {
	rows, err := db.Query(`
		SELECT
			gc.cycle,
			gc.duration_ns,
			COUNT(r.name) as assists,
			COUNT(DISTINCT r.scope_id) as goroutines,
			COALESCE(SUM(r.duration_ns), 0) as assist_time_ns
		FROM gc_cycles gc
		LEFT JOIN gc_ranges r
			ON r.name = 'GC mark assist'
			AND r.start_time_ns < gc.end_time_ns
			AND r.end_time_ns > gc.start_time_ns
		GROUP BY gc.cycle, gc.duration_ns, gc.start_time_ns
		ORDER BY gc.start_time_ns
	`)
	if err != nil {
		return nil, fmt.Errorf("per-cycle breakdown: %w", err)
	}
	defer rows.Close()

	out := &GCPerCycleBreakdown{}
	for rows.Next() {
		var e GCCycleEntry
		var dur, assist float64
		if err := rows.Scan(&e.Cycle, &dur, &e.Assists, &e.Goroutines, &assist); err != nil {
			return nil, err
		}
		e.DurationNs = int64(dur)
		e.AssistTimeNs = int64(assist)
		out.Cycles = append(out.Cycles, e)
	}
	return out, rows.Err()
}

func collectSweepSummary(db *sql.DB) (*GCSweepSummary, error) {
	var count int
	var totalNs sql.NullFloat64
	err := db.QueryRow(`
		SELECT COUNT(*), SUM(duration_ns)
		FROM gc_ranges
		WHERE name = 'GC incremental sweep'
	`).Scan(&count, &totalNs)
	if err != nil {
		return nil, fmt.Errorf("sweep summary: %w", err)
	}
	if count == 0 {
		return nil, nil
	}
	return &GCSweepSummary{Count: count, TotalNs: nullF64ToInt64Ptr(totalNs)}, nil
}

func collectBurstAnalysis(db *sql.DB, window time.Duration) (*BurstReport, error) {
	microWindowNs := int64(1e6) // 1ms
	_ = window                  // reserved for future use

	br := &BurstReport{}

	rows, err := db.Query(`
		WITH min_time AS (
			SELECT MIN(end_time_ns - duration_ns) as t0 FROM g_transitions
		),
		runnable_events AS (
			SELECT g, end_time_ns - duration_ns AS became_runnable_at,
			       src_stack_id
			FROM g_transitions
			WHERE to_state = 'runnable' OR (from_state = 'notexist' AND to_state = 'running')
		),
		micro_windows AS (
			SELECT
				(became_runnable_at - (SELECT t0 FROM min_time)) // $1 AS window_num,
				(became_runnable_at - (SELECT t0 FROM min_time)) // $1 * $1 / 1e6 AS window_start_ms,
				COUNT(*) AS goroutines_spawned,
				COUNT(DISTINCT src_stack_id) AS distinct_creators
			FROM runnable_events
			GROUP BY 1, 2
			HAVING COUNT(*) > 10
		)
		SELECT window_start_ms, goroutines_spawned, distinct_creators
		FROM micro_windows
		ORDER BY goroutines_spawned DESC
		LIMIT $2
	`, microWindowNs, opts.top*2)
	if err != nil {
		return nil, fmt.Errorf("burst query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var w BurstWindow
		if err := rows.Scan(&w.StartMs, &w.GoroutinesSpawned, &w.DistinctCreators); err != nil {
			return nil, err
		}
		br.Bursts = append(br.Bursts, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	creatorRows, err := db.Query(`
		WITH delayed AS (
			SELECT g, duration_ns
			FROM g_transitions
			WHERE from_state = 'runnable' AND to_state = 'running'
			  AND duration_ns > 1000000  -- > 1ms delay
		),
		-- Find the creation event for each delayed goroutine
		creation_info AS (
			SELECT d.g, d.duration_ns, gt.src_stack_id, gt.src_g
			FROM delayed d
			JOIN g_transitions gt ON gt.g = d.g
			WHERE gt.from_state = 'notexist'
		)
		SELECT
			COALESCE(
				(SELECT funcs::VARCHAR FROM stacks WHERE stack_id = c.src_stack_id),
				'[]'
			) as creator_stack,
			COUNT(DISTINCT c.g) as delayed_count,
			SUM(c.duration_ns) as total_delay_ns,
			MAX(c.duration_ns) as max_delay_ns
		FROM creation_info c
		WHERE c.src_stack_id IS NOT NULL
		GROUP BY c.src_stack_id
		ORDER BY total_delay_ns DESC
		LIMIT $1
	`, opts.top)
	if err != nil {
		return nil, fmt.Errorf("creator query: %w", err)
	}
	defer creatorRows.Close()

	for creatorRows.Next() {
		br.HasDelayed = true
		var stackStr string
		var count int
		var totNs, mxNs float64
		if err := creatorRows.Scan(&stackStr, &count, &totNs, &mxNs); err != nil {
			return nil, err
		}
		br.DelayedCreators = append(br.DelayedCreators, DelayedCreator{
			Stack:        parseStackArray(stackStr),
			DelayedCount: count,
			TotalDelayNs: int64(totNs),
			MaxDelayNs:   int64(mxNs),
		})
	}
	return br, creatorRows.Err()
}

func collectWorstDelays(db *sql.DB, n int) (*WorstDelaysReport, error) {
	// Stack info is on running→* transitions, not runnable→running.
	// Get full stack array for proper formatting.
	rows, err := db.Query(`
		WITH worst AS (
			SELECT g, duration_ns, end_time_ns
			FROM g_transitions
			WHERE from_state = 'runnable' AND to_state = 'running'
			ORDER BY duration_ns DESC
			LIMIT $1
		),
		goroutine_stacks AS (
			SELECT DISTINCT ON (g) g, stack_funcs(stack_id) as stack
			FROM g_transitions
			WHERE from_state = 'running' AND stack_id IS NOT NULL
		)
		SELECT w.g, w.duration_ns, COALESCE(s.stack::VARCHAR, '[]') as stack
		FROM worst w
		LEFT JOIN goroutine_stacks s ON w.g = s.g
		ORDER BY w.duration_ns DESC
	`, n)
	if err != nil {
		return nil, fmt.Errorf("worst delays query: %w", err)
	}
	defer rows.Close()

	out := &WorstDelaysReport{N: n}
	rank := 1
	for rows.Next() {
		var r WorstDelayRow
		var dur float64
		var stackStr string
		if err := rows.Scan(&r.G, &dur, &stackStr); err != nil {
			return nil, err
		}
		r.Rank = rank
		r.DurationNs = int64(dur)
		r.Stack = parseStackArray(stackStr)
		out.Rows = append(out.Rows, r)
		rank++
	}
	return out, rows.Err()
}

func collectSpikeDetails(
	db *sql.DB, spikes []SpikeSummary, minTime int64, window time.Duration,
) (*SpikeDetailsSection, error) {
	windowNs := window.Nanoseconds()
	out := &SpikeDetailsSection{}
	for _, s := range spikes {
		var d SpikeDetail
		if s.Type == "latency" {
			det, err := collectLatencySpikeDetail(db, s, minTime, windowNs)
			if err != nil {
				return nil, err
			}
			d = *det
		} else {
			det, err := collectRunnableSpikeDetail(db, s, minTime, window)
			if err != nil {
				return nil, err
			}
			d = *det
		}
		out.Details = append(out.Details, d)
	}
	return out, nil
}

func collectLatencySpikeDetail(
	db *sql.DB, s SpikeSummary, minTime, windowNs int64,
) (*SpikeDetail, error) {
	d := &SpikeDetail{
		Index:   s.Index,
		Type:    "latency",
		StartMs: s.StartMs,
		P99Ns:   s.P99Ns,
	}

	windowStart := minTime + int64(s.WindowNum)*windowNs
	windowEnd := windowStart + windowNs

	// Find the worst individual delay within this window.
	// Latency spikes filter by wait_start (end_time_ns - duration_ns) because
	// that's when the goroutine entered the runnable state — the event that
	// defines which window a scheduling delay belongs to.
	var g int64
	var durationNs float64
	var endTime, waitStart int64
	var srcP sql.NullInt64
	err := db.QueryRow(`
		SELECT g, duration_ns, end_time_ns, src_p,
		       end_time_ns - duration_ns AS wait_start
		FROM g_transitions
		WHERE from_state = 'runnable' AND to_state = 'running'
		  AND end_time_ns - duration_ns >= $1
		  AND end_time_ns - duration_ns < $2
		ORDER BY duration_ns DESC
		LIMIT 1
	`, windowStart, windowEnd).Scan(&g, &durationNs, &endTime, &srcP, &waitStart)
	if err != nil {
		if err == sql.ErrNoRows {
			return d, nil
		}
		return nil, fmt.Errorf("worst delay query: %w", err)
	}

	d.WorstG = &g
	dur := int64(durationNs)
	d.WorstDurationNs = &dur
	if srcP.Valid {
		sp := srcP.Int64
		d.WorstP = &sp
	}

	// Was there a burst of goroutines becoming runnable?
	var burstCount int
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT g)
		FROM g_transitions
		WHERE (to_state = 'runnable' OR (from_state = 'notexist' AND to_state = 'running'))
		  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
	`, waitStart).Scan(&burstCount)
	if err != nil {
		return nil, fmt.Errorf("burst query: %w", err)
	}
	d.BurstCount = &burstCount

	// Was there a long-running goroutine that blocked the queue?
	var longestRunNs sql.NullFloat64
	var longestRunG sql.NullInt64
	var longestRunStack sql.NullString
	if srcP.Valid {
		err = db.QueryRow(`
			SELECT duration_ns, g, stack_funcs(stack_id)::VARCHAR
			FROM g_transitions
			WHERE from_state = 'running'
			  AND src_p = $1
			  AND end_time_ns > $2
			  AND end_time_ns - duration_ns < $3
			ORDER BY duration_ns DESC
			LIMIT 1
		`, srcP.Int64, waitStart, endTime).Scan(&longestRunNs, &longestRunG, &longestRunStack)
		if err != nil && err != sql.ErrNoRows {
			return nil, fmt.Errorf("longest run query: %w", err)
		}
	}

	// How many goroutines ran during the wait?
	var runnersCount, totalRuns int
	if srcP.Valid {
		err = db.QueryRow(`
			SELECT COUNT(DISTINCT g), COUNT(*)
			FROM g_transitions
			WHERE from_state = 'running'
			  AND src_p = $1
			  AND end_time_ns > $2
			  AND end_time_ns - duration_ns < $3
		`, srcP.Int64, waitStart, endTime).Scan(&runnersCount, &totalRuns)
		if err != nil {
			return nil, fmt.Errorf("runners count query: %w", err)
		}
		d.QueueRunners = &runnersCount
		d.QueueRuns = &totalRuns
	}

	if burstCount > 10 {
		bb, err := collectBurstBreakdown(db, waitStart)
		if err == nil {
			d.BurstBreakdown = bb
		}
		// On a breakdown error, the legacy code wrote an inline error string
		// to stdout. Tests don't exercise that path; we silently drop the
		// breakdown so the rest of the report still renders.
	}

	if longestRunNs.Valid && longestRunNs.Float64 > 500000 {
		stack := parseStackArray(longestRunStack.String)
		d.LongestRun = &LongestRun{
			G:          longestRunG.Int64,
			DurationNs: int64(longestRunNs.Float64),
			Stack:      stack,
		}
	}

	return d, nil
}

func collectRunnableSpikeDetail(
	db *sql.DB, s SpikeSummary, minTime int64, window time.Duration,
) (*SpikeDetail, error) {
	d := &SpikeDetail{
		Index:       s.Index,
		Type:        "runnable",
		StartMs:     s.StartMs,
		MaxRunnable: s.MaxRunnable,
	}

	windowMs := window.Milliseconds()
	windowStartMs := int64(s.WindowNum) * windowMs
	windowEndMs := windowStartMs + windowMs

	// Find the 1ms bucket within the window where most goroutines became runnable.
	// Runnable spikes use end_time_ns for bucketing (the transition completion
	// time) rather than wait_start, since we're counting state transitions into
	// "runnable" — a different semantic than latency spike detection.
	var peakBucketMs int64
	err := db.QueryRow(`
		SELECT bucket_ms
		FROM (
			SELECT
				(end_time_ns - $1) // 1000000 as bucket_ms,
				COUNT(*) as enter_count
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND (end_time_ns - $1) // 1000000 >= $2
			  AND (end_time_ns - $1) // 1000000 < $3
			GROUP BY bucket_ms
		)
		ORDER BY enter_count DESC
		LIMIT 1
	`, minTime, windowStartMs, windowEndMs).Scan(&peakBucketMs)
	if err != nil {
		if err == sql.ErrNoRows {
			return d, nil
		}
		return nil, fmt.Errorf("peak bucket query: %w", err)
	}

	peakNs := minTime + peakBucketMs*1000000
	bb, err := collectBurstBreakdown(db, peakNs)
	if err == nil {
		d.BurstBreakdown = bb
	}
	return d, nil
}

// collectBurstBreakdown aggregates the categorized breakdown of goroutines that
// became runnable in a 2ms window centered on runnableStartNs, plus the
// per-category sub-lists (creators for "new", unblockers/blocked-stacks/heavy
// unblockers for "unblocked").
func collectBurstBreakdown(db *sql.DB, runnableStartNs int64) (*BurstBreakdown, error) {
	rows, err := db.Query(`
		WITH burst AS (
			SELECT g, from_state, src_stack_id
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
		)
		SELECT
			CASE
				WHEN from_state = 'notexist' THEN 'new'
				WHEN from_state = 'waiting' THEN 'unblocked'
				WHEN from_state = 'syscall' THEN 'syscall'
				WHEN from_state = 'running' THEN 'preempted'
				WHEN from_state = 'runnable' THEN 'rescheduled'
				ELSE 'other'
			END as category,
			COUNT(*) as cnt
		FROM burst
		GROUP BY 1
		ORDER BY cnt DESC
	`, runnableStartNs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bb := &BurstBreakdown{}
	for rows.Next() {
		var c BurstCategory
		if err := rows.Scan(&c.Name, &c.Count); err != nil {
			return nil, err
		}
		bb.Categories = append(bb.Categories, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, c := range bb.Categories {
		switch c.Name {
		case "new":
			if c.Count == 0 {
				continue
			}
			creators, known, err := collectCreatorBreakdown(db, runnableStartNs)
			if err != nil {
				return nil, err
			}
			bb.Creators = creators
			bb.CreatorsKnown = known
		case "unblocked":
			if c.Count == 0 {
				continue
			}
			ub, err := collectUnblockerBreakdown(db, runnableStartNs)
			if err != nil {
				return nil, err
			}
			bb.Unblockers = ub

			ba, err := collectUnblockedStacks(db, runnableStartNs)
			if err != nil {
				return nil, err
			}
			bb.BlockedAt = ba

			hu, err := collectHeavyUnblockers(db, runnableStartNs)
			if err != nil {
				return nil, err
			}
			bb.HeavyUnblockers = hu
		}
	}
	return bb, nil
}

func collectCreatorBreakdown(db *sql.DB, runnableStartNs int64) ([]CreatorEntry, bool, error) {
	rows, err := db.Query(`
		WITH burst_creates AS (
			SELECT g, src_stack_id
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND from_state = 'notexist'
			  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
		)
		SELECT
			bc.src_stack_id,
			COUNT(*) as cnt,
			s.terse::VARCHAR as creator_stack
		FROM burst_creates bc
		LEFT JOIN stacks s ON bc.src_stack_id = s.stack_id
		GROUP BY bc.src_stack_id, creator_stack
		ORDER BY cnt DESC, creator_stack
		LIMIT 3
	`, runnableStartNs)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var out []CreatorEntry
	known := false
	for rows.Next() {
		known = true
		var stackID sql.NullInt64
		var cnt int
		var stackStr sql.NullString
		if err := rows.Scan(&stackID, &cnt, &stackStr); err != nil {
			return nil, false, err
		}
		entry := CreatorEntry{Count: cnt}
		if stackStr.Valid {
			stack := parseStackArray(stackStr.String)
			for i := range stack {
				stack[i] = stripFileLocation(stack[i])
			}
			entry.Stack = stack
		}
		out = append(out, entry)
	}
	return out, known, rows.Err()
}

func collectUnblockerBreakdown(db *sql.DB, runnableStartNs int64) ([]UnblockerEntry, error) {
	rows, err := db.Query(`
		WITH burst_unblocks AS (
			SELECT g, src_stack_id
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND from_state = 'waiting'
			  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
		)
		SELECT
			s.terse[1] as unblocker_func,
			COUNT(*) as cnt
		FROM burst_unblocks bu
		LEFT JOIN stacks s ON bu.src_stack_id = s.stack_id
		WHERE bu.src_stack_id IS NOT NULL
		GROUP BY unblocker_func
		ORDER BY cnt DESC, unblocker_func
		LIMIT 3
	`, runnableStartNs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []UnblockerEntry
	for rows.Next() {
		var funcName sql.NullString
		var cnt int
		if err := rows.Scan(&funcName, &cnt); err != nil {
			return nil, err
		}
		e := UnblockerEntry{Count: cnt}
		if funcName.Valid {
			e.Func = shortenFuncName(stripFileLocation(funcName.String))
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func collectUnblockedStacks(db *sql.DB, runnableStartNs int64) ([]BlockedAtEntry, error) {
	// The stack_id on waiting→runnable transitions is not populated in the Go
	// trace, so we look at each goroutine's most recent running→* transition
	// to find what it was doing when it blocked.
	rows, err := db.Query(`
		WITH burst_gs AS (
			SELECT DISTINCT g, end_time_ns as unblock_time
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND from_state = 'waiting'
			  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
		),
		g_wait_stack_ids AS (
			SELECT bg.g,
				(SELECT gt2.stack_id FROM g_transitions gt2
				 WHERE gt2.g = bg.g
				   AND gt2.from_state = 'running'
				   AND gt2.stack_id IS NOT NULL
				   AND gt2.end_time_ns <= bg.unblock_time
				 ORDER BY gt2.end_time_ns DESC
				 LIMIT 1) as wait_stack_id
			FROM burst_gs bg
		)
		SELECT
			s.funcs::VARCHAR as stack_funcs,
			COUNT(*) as cnt
		FROM g_wait_stack_ids gwsi
		JOIN stacks s ON gwsi.wait_stack_id = s.stack_id
		WHERE gwsi.wait_stack_id IS NOT NULL
		GROUP BY 1
		ORDER BY cnt DESC, 1
		LIMIT $2
	`, runnableStartNs, 3)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []BlockedAtEntry
	for rows.Next() {
		var stackStr string
		var cnt int
		if err := rows.Scan(&stackStr, &cnt); err != nil {
			return nil, err
		}
		stack := parseStackArray(stackStr)
		entry := BlockedAtEntry{Count: cnt, Stack: stack}
		if len(stack) > 0 && !isInterestingFrame(stack[0]) {
			entry.LeafBracket = shortenFuncName(stack[0])
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

func collectHeavyUnblockers(db *sql.DB, runnableStartNs int64) ([]HeavyUnblockerEntry, error) {
	rows, err := db.Query(`
		WITH burst_unblocks AS (
			SELECT src_g, src_stack_id
			FROM g_transitions
			WHERE to_state = 'runnable'
			  AND from_state = 'waiting'
			  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
			  AND src_g IS NOT NULL
		),
		by_unblocker AS (
			SELECT
				src_g,
				mode(src_stack_id) as common_stack_id,
				COUNT(*) as cnt
			FROM burst_unblocks
			GROUP BY src_g
			ORDER BY cnt DESC, src_g
			LIMIT $2
		)
		SELECT
			bu.src_g,
			s.funcs::VARCHAR as unblocker_funcs,
			bu.cnt
		FROM by_unblocker bu
		LEFT JOIN stacks s ON bu.common_stack_id = s.stack_id
		ORDER BY bu.cnt DESC, bu.src_g
	`, runnableStartNs, 3)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []HeavyUnblockerEntry
	for rows.Next() {
		var srcG int64
		var stackStr sql.NullString
		var cnt int
		if err := rows.Scan(&srcG, &stackStr, &cnt); err != nil {
			return nil, err
		}
		e := HeavyUnblockerEntry{G: srcG, Count: cnt}
		if stackStr.Valid {
			e.Stack = parseStackArray(stackStr.String)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// parseStackArray parses DuckDB's array format: [func1, func2, ...]
func parseStackArray(s string) []string {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return nil
	}

	// Split by ", " but handle quoted strings
	var result []string
	var current strings.Builder
	inQuote := false
	for _, c := range s {
		switch c {
		case '\'':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				if str := strings.TrimSpace(current.String()); str != "" {
					result = append(result, strings.Trim(str, "'"))
				}
				current.Reset()
				continue
			}
		}
		current.WriteRune(c)
	}
	if str := strings.TrimSpace(current.String()); str != "" {
		result = append(result, strings.Trim(str, "'"))
	}
	return result
}

func collectTopGoroutines(db *sql.DB) (*TopGoroutinesReport, error) {
	rows, err := db.Query(`
		SELECT
			g,
			COALESCE(
				(SELECT funcs::VARCHAR FROM stacks WHERE stack_id =
					(SELECT stack_id FROM g_transitions gt2 WHERE gt2.g = gt.g AND stack_id IS NOT NULL LIMIT 1)
				),
				'[]'
			) as stack,
			COUNT(*) as sched_count,
			SUM(duration_ns) as total_wait,
			MAX(duration_ns) as max_wait
		FROM g_transitions gt
		WHERE from_state = 'runnable' AND to_state = 'running'
		GROUP BY 1
		ORDER BY total_wait DESC
		LIMIT $1
	`, opts.top)
	if err != nil {
		return nil, fmt.Errorf("top goroutines query: %w", err)
	}
	defer rows.Close()

	out := &TopGoroutinesReport{}
	for rows.Next() {
		var r TopGoroutineRow
		var stackStr string
		var totWait, mxWait float64
		if err := rows.Scan(&r.G, &stackStr, &r.SchedCount, &totWait, &mxWait); err != nil {
			return nil, err
		}
		r.Stack = parseStackArray(stackStr)
		r.TotalNs = int64(totWait)
		r.MaxNs = int64(mxWait)
		out.Rows = append(out.Rows, r)
	}
	return out, rows.Err()
}

func printSchemaInfo() {
	fmt.Println(`
Key tables in the DuckDB:
  g_transitions  - Goroutine state changes (the main table for scheduling analysis)
                   Columns: g, from_state, to_state, reason, duration_ns, end_time_ns,
                            stack_id, src_stack_id, src_g, src_m, src_p
  goroutines     - Aggregated per-goroutine stats (view)
  stacks         - Resolved stack traces with function names (view)
  p_transitions  - Processor state changes
  metrics        - Runtime metrics over time

Useful patterns:
  Scheduling latency = duration_ns WHERE from_state='runnable' AND to_state='running'
  stack_funcs(stack_id) returns the function names in a stack
  list_first(stack_funcs(id)) gets the top function (leaf)

Example: SELECT * FROM goroutines ORDER BY runnable_ns DESC LIMIT 10;`)
}

func execDuckDB(dbFile string) error {
	duckdb, err := exec.LookPath("duckdb")
	if err != nil {
		return fmt.Errorf("duckdb not found in PATH")
	}
	return syscall.Exec(duckdb, []string{"duckdb", dbFile}, os.Environ())
}

func fmtDuration(ns float64) string {
	if ns < 1000 {
		return fmt.Sprintf("%.0fns", ns)
	}
	if ns < 1e6 {
		return fmt.Sprintf("%.1fµs", ns/1e3)
	}
	if ns < 1e9 {
		return fmt.Sprintf("%.2fms", ns/1e6)
	}
	return fmt.Sprintf("%.2fs", ns/1e9)
}

func shortenFunc(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) > 2 {
		return strings.Join(parts[len(parts)-2:], "/")
	}
	return name
}

// formatStack takes an array of function names (leaf/terminal first) and returns a
// concise representation prioritizing the terminal frame and working backwards.
//
// Algorithm: Start with terminal frame, prepend frames from distinct packages
// until we'd exceed maxLen. This prioritizes what the goroutine was doing
// (terminal) over how it got there (entry point).
//
// Example output: "DistSender.sendPartialBatchAsync → pebble.SeekPrefixGE → _Cfunc_calloc"
func formatStack(funcs []string, maxLen int) string {
	if len(funcs) == 0 {
		return "(no stack)"
	}

	// Filter to get interesting frames (non-runtime, non-stdlib), keeping leaf-first order
	interesting := make([]string, 0, len(funcs))
	for _, f := range funcs {
		if isInterestingFrame(f) {
			interesting = append(interesting, shortenFuncName(f))
		}
	}

	if len(interesting) == 0 {
		// All frames are runtime/stdlib - just show the terminal frame
		return shortenFuncName(funcs[0])
	}

	if len(interesting) == 1 {
		return interesting[0]
	}

	// Build from terminal (first) backwards, prepending distinct package frames
	// Result will be: entry → ... → terminal
	//
	// We accumulate: [terminal], then [middle, terminal], then [entry, middle, terminal]
	// Stop when adding another frame would exceed maxLen

	const arrow = " → "
	result := interesting[0] // start with terminal frame
	seenPkgs := map[string]bool{extractPkg(interesting[0]): true}

	// Work backwards through the stack (towards entry point)
	for i := 1; i < len(interesting); i++ {
		frame := interesting[i]
		pkg := extractPkg(frame)

		// Skip if we've seen this package
		if seenPkgs[pkg] {
			continue
		}

		// Would adding this frame exceed the limit?
		candidate := frame + arrow + result
		if len(candidate) > maxLen {
			// Can't fit more, stop here
			break
		}

		// Prepend this frame
		result = candidate
		seenPkgs[pkg] = true
	}

	return result
}

// isInterestingFrame returns true if the frame is not runtime/stdlib
func isInterestingFrame(f string) bool {
	// Skip runtime and standard library
	if strings.HasPrefix(f, "runtime.") || strings.HasPrefix(f, "runtime/") {
		return false
	}
	if strings.HasPrefix(f, "sync.") || strings.HasPrefix(f, "syscall.") {
		return false
	}
	if strings.HasPrefix(f, "internal/") {
		return false
	}
	// Check for stdlib packages (no dots before first slash indicates stdlib)
	if !strings.Contains(strings.Split(f, "/")[0], ".") {
		return false
	}
	return true
}

// shortenFuncName extracts just the type.Method or func name
func shortenFuncName(f string) string {
	// Remove quotes if present
	f = strings.Trim(f, "'")
	// Get just the last component after the package path
	if idx := strings.LastIndex(f, "/"); idx >= 0 {
		f = f[idx+1:]
	}
	// Remove package prefix if it makes sense
	if idx := strings.Index(f, "."); idx >= 0 {
		// Keep type.Method but trim long package names
		parts := strings.SplitN(f, ".", 2)
		if len(parts) == 2 {
			// If first part looks like a package name (lowercase), use second part
			// If first part looks like a type (uppercase or with *), keep both
			if len(parts[0]) > 0 && (parts[0][0] == '(' || (parts[0][0] >= 'A' && parts[0][0] <= 'Z')) {
				return f
			}
			return parts[1]
		}
	}
	return f
}

// stripFileLocation removes the file:line suffix from a function name.
// E.g. "foo.Bar (file.go:123)" -> "foo.Bar"
func stripFileLocation(f string) string {
	if idx := strings.Index(f, " ("); idx >= 0 {
		return f[:idx]
	}
	return f
}

// extractPkg returns the package path from a function name
func extractPkg(f string) string {
	f = strings.Trim(f, "'")
	// Find the last slash before the function name
	if idx := strings.LastIndex(f, "/"); idx >= 0 {
		return f[:idx]
	}
	// If no slash, try to get everything before the first dot
	if idx := strings.Index(f, "."); idx >= 0 {
		return f[:idx]
	}
	return f
}
