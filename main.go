// schedstat analyzes Go execution traces for scheduling latency.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"io"

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
	f.BoolVar(&opts.gc, "gc", false, "show GC-related state transitions")
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

	if err := analyze(database, traceFile, os.Stdout); err != nil {
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

	return analyze(db.DB, traceFile, w)
}

func analyze(db *sql.DB, traceFile string, w io.Writer) error {
	fmt.Fprintf(w, "schedstat: %s\n", filepath.Base(traceFile))
	fmt.Fprintln(w, strings.Repeat("=", 60))

	// Get time bounds
	var minTime, maxTime int64
	err := db.QueryRow(`
		SELECT MIN(end_time_ns - duration_ns), MAX(end_time_ns)
		FROM g_transitions
		WHERE from_state = 'runnable' AND to_state = 'running'
	`).Scan(&minTime, &maxTime)
	if err != nil {
		return fmt.Errorf("getting time bounds: %w", err)
	}
	durationMs := float64(maxTime-minTime) / 1e6
	fmt.Fprintf(w, "\nTrace duration: %.1fms\n", durationMs)

	// Get processor count for goroutine threshold default
	var procCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM procs`).Scan(&procCount); err != nil {
		return fmt.Errorf("getting processor count: %w", err)
	}
	goroutineThreshold := opts.goroutineThreshold
	if goroutineThreshold == 0 {
		goroutineThreshold = 5 * procCount
	}

	// Always show overall stats
	if err := printOverallStats(db, w); err != nil {
		return err
	}

	// Time series if requested
	if opts.timeseries {
		if err := printTimeseries(db, w, minTime, opts.window); err != nil {
			return err
		}
	}

	// Spike detection.
	latencySpikes, totalLatency, err := queryLatencySpikes(db, minTime, opts.window, opts.spikeThreshold, opts.top)
	if err != nil {
		return err
	}
	runnableSpikes, totalRunnable, err := queryRunnableSpikes(db, minTime, opts.window, goroutineThreshold, opts.top)
	if err != nil {
		return err
	}

	// Assign display indices before printing so print functions are pure output.
	idx := 1
	for i := range latencySpikes {
		latencySpikes[i].index = idx
		idx++
	}
	for i := range runnableSpikes {
		runnableSpikes[i].index = idx
		idx++
	}

	printLatencySpikesSection(w, latencySpikes, totalLatency, opts.window, opts.spikeThreshold)
	printRunnableSpikesSection(w, runnableSpikes, totalRunnable, opts.window, goroutineThreshold)

	allSpikes := make([]spikeInfo, 0, len(latencySpikes)+len(runnableSpikes))
	allSpikes = append(allSpikes, latencySpikes...)
	allSpikes = append(allSpikes, runnableSpikes...)
	if len(allSpikes) > 0 {
		if err := printSpikeDetails(db, w, allSpikes, minTime, opts.window); err != nil {
			return err
		}
	}

	// By-creator analysis
	if opts.byCreator {
		if err := printByCreator(db, w); err != nil {
			return err
		}
	}

	// GC analysis
	if opts.gc {
		if err := printGCAnalysis(db, w); err != nil {
			return err
		}
	}

	// Burst analysis
	if opts.bursts {
		if err := printBurstAnalysis(db, w, opts.window); err != nil {
			return err
		}
	}

	// Worst delays
	if opts.worst > 0 {
		if err := printWorstDelays(db, w, opts.worst); err != nil {
			return err
		}
	}

	// Top goroutines by wait time (optional, can be slow)
	if opts.topWaiters {
		if err := printTopGoroutines(db, w); err != nil {
			return err
		}
	}

	return nil
}

func printOverallStats(db *sql.DB, w io.Writer) error {
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
		return fmt.Errorf("getting overall stats: %w", err)
	}

	fmt.Fprintln(w, "\n--- Scheduling Latency (runnable → running) ---")
	fmt.Fprintf(w, "Events: %d\n", count)
	if count > 0 {
		fmt.Fprintf(w, "  min: %-10s  p50: %-10s  p90: %-10s\n",
			fmtNullDuration(minNs), fmtNullDuration(p50Ns), fmtNullDuration(p90Ns))
		fmt.Fprintf(w, "  avg: %-10s  p99: %-10s  max: %-10s\n",
			fmtNullDuration(avgNs), fmtNullDuration(p99Ns), fmtNullDuration(maxNs))
	}
	return nil
}

func printTimeseries(db *sql.DB, w io.Writer, minTime int64, window time.Duration) error {
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
		return fmt.Errorf("timeseries query: %w", err)
	}
	defer rows.Close()

	fmt.Fprintf(w, "\n--- Latency by %s Window ---\n", window)
	fmt.Fprintf(w, "%-8s %-10s %-10s %-8s %-12s\n", "Window", "Start(ms)", "End(ms)", "Events", "p99")
	fmt.Fprintf(w, "%-8s %-10s %-10s %-8s %-12s\n", "------", "---------", "-------", "------", "---")

	for rows.Next() {
		var windowNum int
		var startMs, endMs float64
		var cnt int
		var p99 float64
		if err := rows.Scan(&windowNum, &startMs, &endMs, &cnt, &p99); err != nil {
			return err
		}
		marker := ""
		if p99 > float64((opts.spikeThreshold).Nanoseconds()) {
			marker = " ←"
		}
		fmt.Fprintf(w, "%-8d %-10.0f %-10.0f %-8d %-12s%s\n",
			windowNum, startMs, endMs, cnt, fmtDuration(p99), marker)
	}
	return rows.Err()
}

type spikeType int

const (
	latencySpike spikeType = iota
	runnableSpike
)

type spikeInfo struct {
	index       int // sequential display number, assigned in analyze()
	windowNum   int
	startMs     float64
	eventCount  int
	p99         float64
	maxLatency  float64
	maxRunnable int
	spikeType   spikeType
}

func queryLatencySpikes(db *sql.DB, minTime int64, window, threshold time.Duration, top int) ([]spikeInfo, int, error) {
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

	var spikes []spikeInfo
	var total int
	for rows.Next() {
		var s spikeInfo
		s.spikeType = latencySpike
		if err := rows.Scan(&s.windowNum, &s.startMs, &s.eventCount, &s.p99, &s.maxLatency, &total); err != nil {
			return nil, 0, err
		}
		spikes = append(spikes, s)
	}
	return spikes, total, rows.Err()
}

func queryRunnableSpikes(db *sql.DB, minTime int64, window time.Duration, runnableThreshold, top int) ([]spikeInfo, int, error) {
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

	var spikes []spikeInfo
	var total int
	for rows.Next() {
		var s spikeInfo
		s.spikeType = runnableSpike
		if err := rows.Scan(&s.windowNum, &s.maxRunnable, &total); err != nil {
			return nil, 0, err
		}
		s.startMs = float64(s.windowNum) * float64(windowMs)
		spikes = append(spikes, s)
	}
	return spikes, total, rows.Err()
}

func printLatencySpikesSection(w io.Writer, spikes []spikeInfo, total int, window, threshold time.Duration) {
	if total == 0 {
		return
	}

	fmt.Fprintf(w, "\n--- Latency Spikes (p99 > %s per %s) ---\n", threshold, window)
	if total > len(spikes) {
		fmt.Fprintf(w, "%d window(s) above threshold (showing top %d)\n", total, len(spikes))
	} else {
		fmt.Fprintf(w, "%d window(s) above threshold\n", total)
	}
	fmt.Fprintln(w)

	for _, s := range spikes {
		fmt.Fprintf(w, "  [%d] t=%.0fms  p99=%s  max=%s  %d events\n",
			s.index, s.startMs, fmtDuration(s.p99), fmtDuration(s.maxLatency), s.eventCount)
	}
}

func printRunnableSpikesSection(w io.Writer, spikes []spikeInfo, total int, window time.Duration, runnableThreshold int) {
	if total == 0 {
		return
	}

	fmt.Fprintf(w, "\n--- Runnable Spikes (>%d runnable per %s) ---\n", runnableThreshold, window)
	if total > len(spikes) {
		fmt.Fprintf(w, "%d window(s) above threshold (showing top %d)\n", total, len(spikes))
	} else {
		fmt.Fprintf(w, "%d window(s) above threshold\n", total)
	}
	fmt.Fprintln(w)

	for _, s := range spikes {
		fmt.Fprintf(w, "  [%d] t=%.0fms  peak %d runnable\n",
			s.index, s.startMs, s.maxRunnable)
	}
}

func printByCreator(db *sql.DB, w io.Writer) error {
	fmt.Fprintln(w, "\n--- Delays by Goroutine Creator ---")

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
		return fmt.Errorf("by-creator query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var creator string
		var cnt int
		var totalWait, maxWait, p99 float64
		if err := rows.Scan(&creator, &cnt, &totalWait, &maxWait, &p99); err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s\n", shortenFunc(creator))
		fmt.Fprintf(w, "    %d events, total: %s, max: %s, p99: %s\n",
			cnt, fmtDuration(totalWait), fmtDuration(maxWait), fmtDuration(p99))
	}
	return rows.Err()
}

func printGCAnalysis(db *sql.DB, w io.Writer) error {
	fmt.Fprintln(w, "\n--- GC-Related Activity ---")

	rows, err := db.Query(`
		SELECT
			from_state,
			to_state,
			reason,
			COUNT(*) as transitions,
			SUM(duration_ns) as total_ns,
			MAX(duration_ns) as max_ns
		FROM g_transitions
		WHERE reason LIKE '%GC%'
		   OR reason LIKE '%gc%'
		   OR from_state LIKE '%gc%'
		   OR to_state LIKE '%gc%'
		GROUP BY 1, 2, 3
		ORDER BY total_ns DESC
		LIMIT $1
	`, opts.top*2)
	if err != nil {
		return fmt.Errorf("gc query: %w", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var fromState, toState, reason string
		var transitions int
		var totalNs, maxNs float64
		if err := rows.Scan(&fromState, &toState, &reason, &transitions, &totalNs, &maxNs); err != nil {
			return err
		}
		fmt.Fprintf(w, "  %s → %s (%s)\n", fromState, toState, reason)
		fmt.Fprintf(w, "    %d transitions, total: %s, max: %s\n",
			transitions, fmtDuration(totalNs), fmtDuration(maxNs))
	}
	if !hasRows {
		fmt.Fprintln(w, "  No GC-related transitions found.")
	}
	return rows.Err()
}

func printBurstAnalysis(db *sql.DB, w io.Writer, window time.Duration) error {
	// Use 1ms micro-windows to detect bursts
	microWindowNs := int64(1e6) // 1ms
	_ = window                  // reserved for future use

	fmt.Fprintf(w, "\n--- Goroutine Bursts (>10 becoming runnable in 1ms) ---\n")

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
		return fmt.Errorf("burst query: %w", err)
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var windowMs float64
		var spawned, distinctCreators int
		if err := rows.Scan(&windowMs, &spawned, &distinctCreators); err != nil {
			return err
		}
		fmt.Fprintf(w, "  t=%-8.1fms: %d goroutines became runnable (%d distinct creators)\n",
			windowMs, spawned, distinctCreators)
	}
	if !hasRows {
		fmt.Fprintln(w, "  No significant bursts detected.")
	}

	// Show the top creators of goroutines that experienced delays
	// Look at the creation event (from_state='notexist') to find who created them
	fmt.Fprintf(w, "\n--- Who Launched Delayed Goroutines? ---\n")

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
		return fmt.Errorf("creator query: %w", err)
	}
	defer creatorRows.Close()

	hasCreators := false
	for creatorRows.Next() {
		hasCreators = true
		var stackStr string
		var count int
		var totalNs, maxNs float64
		if err := creatorRows.Scan(&stackStr, &count, &totalNs, &maxNs); err != nil {
			return err
		}
		stack := parseStackArray(stackStr)
		fmt.Fprintf(w, "  %s\n", formatStack(stack, 80))
		fmt.Fprintf(w, "    launched %d delayed goroutines, total delay: %s, max: %s\n",
			count, fmtDuration(totalNs), fmtDuration(maxNs))
	}
	if !hasCreators {
		fmt.Fprintln(w, "  (no creator info available)")
	}

	return creatorRows.Err()
}

func printWorstDelays(db *sql.DB, w io.Writer, n int) error {
	fmt.Fprintf(w, "\n--- %d Worst Individual Delays ---\n", n)

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
		return fmt.Errorf("worst delays query: %w", err)
	}
	defer rows.Close()

	rank := 1
	for rows.Next() {
		var g int64
		var durationNs float64
		var stackStr string
		if err := rows.Scan(&g, &durationNs, &stackStr); err != nil {
			return err
		}
		stack := parseStackArray(stackStr)
		fmt.Fprintf(w, "\n%d. G%d waited %s\n", rank, g, fmtDuration(durationNs))
		fmt.Fprintf(w, "   %s\n", formatStack(stack, 100))
		rank++
	}
	return rows.Err()
}

func printSpikeDetails(db *sql.DB, w io.Writer, spikes []spikeInfo, minTime int64, window time.Duration) error {
	fmt.Fprintf(w, "\n--- Spike Details ---\n")

	windowNs := window.Nanoseconds()

	for _, s := range spikes {
		fmt.Fprintln(w)
		if s.spikeType == latencySpike {
			fmt.Fprintf(w, "[%d] t=%.0fms [latency] p99=%s\n", s.index, s.startMs, fmtDuration(s.p99))
			if err := printLatencySpikeDetail(db, w, s, minTime, windowNs); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(w, "[%d] t=%.0fms [runnable] peak %d runnable\n", s.index, s.startMs, s.maxRunnable)
			if err := printRunnableSpikeDetail(db, w, s, minTime, window); err != nil {
				return err
			}
		}
	}

	return nil
}

func printLatencySpikeDetail(db *sql.DB, w io.Writer, s spikeInfo, minTime, windowNs int64) error {
	windowStart := minTime + int64(s.windowNum)*windowNs
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
			return nil
		}
		return fmt.Errorf("worst delay query: %w", err)
	}

	fmt.Fprintf(w, "   → G%d waited %s", g, fmtDuration(durationNs))
	if srcP.Valid {
		fmt.Fprintf(w, " on P%d", srcP.Int64)
	}
	fmt.Fprintln(w)

	// Was there a burst of goroutines becoming runnable?
	var burstCount int
	err = db.QueryRow(`
		SELECT COUNT(DISTINCT g)
		FROM g_transitions
		WHERE (to_state = 'runnable' OR (from_state = 'notexist' AND to_state = 'running'))
		  AND end_time_ns - duration_ns BETWEEN $1 - 1000000 AND $1 + 1000000
	`, waitStart).Scan(&burstCount)
	if err != nil {
		return fmt.Errorf("burst query: %w", err)
	}

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
			return fmt.Errorf("longest run query: %w", err)
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
			return fmt.Errorf("runners count query: %w", err)
		}
	}

	// Report findings
	if burstCount > 10 {
		fmt.Fprintf(w, "   → Burst: %d goroutines became runnable within ±1ms\n", burstCount)
		if err := printBurstBreakdown(db, w, waitStart); err != nil {
			fmt.Fprintf(w, "     (burst breakdown error: %v)\n", err)
		}
	}

	if longestRunNs.Valid && longestRunNs.Float64 > 500000 { // > 500µs
		stack := parseStackArray(longestRunStack.String)
		fmt.Fprintf(w, "   → Longest run during wait: G%d ran %s\n",
			longestRunG.Int64, fmtDuration(longestRunNs.Float64))
		if len(stack) > 0 {
			fmt.Fprintf(w, "     %s\n", formatStack(stack, 80))
		}
	}

	if runnersCount > 0 {
		fmt.Fprintf(w, "   → Queue activity: %d goroutines ran %d times during the wait\n",
			runnersCount, totalRuns)
	}

	if burstCount <= 10 && (!longestRunNs.Valid || longestRunNs.Float64 <= 500000) && runnersCount == 0 {
		fmt.Fprintln(w, "   → No clear single cause identified")
	}

	return nil
}

func printRunnableSpikeDetail(db *sql.DB, w io.Writer, s spikeInfo, minTime int64, window time.Duration) error {
	windowMs := window.Milliseconds()
	windowStartMs := int64(s.windowNum) * windowMs
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
			return nil
		}
		return fmt.Errorf("peak bucket query: %w", err)
	}

	peakNs := minTime + peakBucketMs*1000000

	if err := printBurstBreakdown(db, w, peakNs); err != nil {
		fmt.Fprintf(w, "     (burst breakdown error: %v)\n", err)
	}

	return nil
}

// printBurstBreakdown shows detailed analysis of goroutines that became runnable
// in a burst around the given timestamp.
func printBurstBreakdown(db *sql.DB, w io.Writer, runnableStartNs int64) error {
	// Get breakdown by category (created vs unblocked vs syscall-return etc)
	// Focus on what's most actionable: new goroutines and unblocked goroutines
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
		return err
	}
	defer rows.Close()

	// Collect category breakdown
	var categories []struct {
		name  string
		count int
	}
	for rows.Next() {
		var cat string
		var cnt int
		if err := rows.Scan(&cat, &cnt); err != nil {
			return err
		}
		categories = append(categories, struct {
			name  string
			count int
		}{cat, cnt})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Print summary line showing the category breakdown
	var parts []string
	for _, c := range categories {
		parts = append(parts, fmt.Sprintf("%d %s", c.count, c.name))
	}
	fmt.Fprintf(w, "     Breakdown: %s\n", strings.Join(parts, ", "))

	// For created goroutines, show who created them
	for _, c := range categories {
		if c.name == "new" && c.count > 0 {
			if err := printCreatorBreakdown(db, w, runnableStartNs); err != nil {
				return err
			}
		}
		if c.name == "unblocked" && c.count > 0 {
			if err := printUnblockerBreakdown(db, w, runnableStartNs); err != nil {
				return err
			}
			if err := printUnblockedGoroutineStacks(db, w, runnableStartNs); err != nil {
				return err
			}
			if err := printHeavyUnblockers(db, w, runnableStartNs); err != nil {
				return err
			}
		}
	}

	return nil
}

// printCreatorBreakdown shows what code paths created the goroutines in a burst.
func printCreatorBreakdown(db *sql.DB, w io.Writer, runnableStartNs int64) error {
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
		return err
	}
	defer rows.Close()

	hasRows := false
	for rows.Next() {
		hasRows = true
		var stackID sql.NullInt64
		var cnt int
		var stackStr sql.NullString
		if err := rows.Scan(&stackID, &cnt, &stackStr); err != nil {
			return err
		}
		if stackStr.Valid {
			stack := parseStackArray(stackStr.String)
			// Strip file locations to make the stack more compact
			for i := range stack {
				stack[i] = stripFileLocation(stack[i])
			}
			fmt.Fprintf(w, "     Created by (%d): %s\n", cnt, formatStack(stack, 90))
		} else {
			fmt.Fprintf(w, "     Created by (%d): (unknown creator)\n", cnt)
		}
	}
	if !hasRows {
		fmt.Fprintln(w, "     (no creator info available)")
	}
	return rows.Err()
}

// printUnblockerBreakdown shows what code paths unblocked the waiting goroutines.
func printUnblockerBreakdown(db *sql.DB, w io.Writer, runnableStartNs int64) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var funcName sql.NullString
		var cnt int
		if err := rows.Scan(&funcName, &cnt); err != nil {
			return err
		}
		if funcName.Valid {
			fn := shortenFuncName(stripFileLocation(funcName.String))
			fmt.Fprintf(w, "     Unblocked by (%d): %s\n", cnt, fn)
		} else {
			fmt.Fprintf(w, "     Unblocked by (%d): (unknown)\n", cnt)
		}
	}
	return rows.Err()
}

// printUnblockedGoroutineStacks shows what the unblocked goroutines were doing
// when they blocked, grouped by their blocking call stack. This identifies
// "worker pool" patterns where many goroutines of the same kind are all waiting
// on the same thing (e.g., 173 raftScheduler workers all blocked on Cond.Wait).
func printUnblockedGoroutineStacks(db *sql.DB, w io.Writer, runnableStartNs int64) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var stackStr string
		var cnt int
		if err := rows.Scan(&stackStr, &cnt); err != nil {
			return err
		}
		stack := parseStackArray(stackStr)
		formatted := formatStack(stack, 80)
		// If the leaf frame is a runtime/sync function that formatStack filters
		// out, show it in brackets so that entries differing only in their
		// blocking primitive (e.g. Cond.Wait vs Mutex.Lock) remain distinct.
		if len(stack) > 0 && !isInterestingFrame(stack[0]) {
			leaf := shortenFuncName(stack[0])
			fmt.Fprintf(w, "     Blocked at (%d): %s [%s]\n", cnt, formatted, leaf)
		} else {
			fmt.Fprintf(w, "     Blocked at (%d): %s\n", cnt, formatted)
		}
	}
	return rows.Err()
}

// printHeavyUnblockers shows individual goroutines that unblocked the most
// other goroutines in the burst window. This pinpoints the root-cause goroutine
// driving the burst (e.g., the raftTickLoop goroutine that wakes hundreds of
// raft scheduler workers via Cond.Broadcast).
func printHeavyUnblockers(db *sql.DB, w io.Writer, runnableStartNs int64) error {
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
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var srcG int64
		var stackStr sql.NullString
		var cnt int
		if err := rows.Scan(&srcG, &stackStr, &cnt); err != nil {
			return err
		}
		if stackStr.Valid {
			stack := parseStackArray(stackStr.String)
			fmt.Fprintf(w, "     Heavy unblocker G%d (%d): %s\n", srcG, cnt, formatStack(stack, 100))
		} else {
			fmt.Fprintf(w, "     Heavy unblocker G%d (%d): (unknown stack)\n", srcG, cnt)
		}
	}
	return rows.Err()
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

func printTopGoroutines(db *sql.DB, w io.Writer) error {
	fmt.Fprintln(w, "\n--- Top Goroutines by Wait Time ---")

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
		return fmt.Errorf("top goroutines query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var g int
		var stackStr string
		var schedCount int
		var totalWait, maxWait float64
		if err := rows.Scan(&g, &stackStr, &schedCount, &totalWait, &maxWait); err != nil {
			return err
		}
		stack := parseStackArray(stackStr)
		fmt.Fprintf(w, "  G%-6d %s\n", g, formatStack(stack, 80))
		fmt.Fprintf(w, "          %d schedulings, total: %s, max: %s\n",
			schedCount, fmtDuration(totalWait), fmtDuration(maxWait))
	}
	return rows.Err()
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

func fmtNullDuration(ns sql.NullFloat64) string {
	if !ns.Valid {
		return "-"
	}
	return fmtDuration(ns.Float64)
}

func shortenFunc(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) > 2 {
		return strings.Join(parts[len(parts)-2:], "/")
	}
	return name
}

func truncateStack(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
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
