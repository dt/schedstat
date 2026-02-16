# schedstat

Analyze Go execution traces and answer: **"What's causing goroutines to wait?"**

`schedstat` converts a Go execution trace into a DuckDB database, runs analysis
queries, and presents the results as formatted output. It detects scheduling
latency spikes and runnable goroutine spikes, then provides per-window root
cause analysis showing burst breakdowns, heavy unblockers, and queue activity.

## Usage

```bash
go install github.com/dt/schedstat@latest

# Basic analysis: summary + spike detection + per-spike details
schedstat trace.out

# Show more spikes
schedstat -n 10 trace.out

# Tuning thresholds
schedstat --spike-threshold=2ms trace.out       # latency spike threshold (default 1ms)
schedstat --runnable-threshold=200 trace.out     # runnable count threshold (default 5*GOMAXPROCS)

# Additional analyses
schedstat --bursts trace.out         # goroutine launch bursts + who launched them
schedstat --worst=20 trace.out       # N worst individual delays with stacks
schedstat --timeseries trace.out     # p99 per time window
schedstat --by-creator trace.out     # delays grouped by goroutine creator
schedstat --gc trace.out             # GC-related state transitions

# Power user
schedstat --sql trace.out            # drop into DuckDB shell after analysis
schedstat --keep-db trace.out        # keep .duckdb file for later exploration
```

## Output

The default output includes:

1. **Overall latency stats** - event count, min, p50, p90, p99, max
2. **Latency Spikes** - windows where p99 exceeded the threshold, ranked by severity
3. **Runnable Spikes** - windows where the runnable goroutine count exceeded the
   threshold, ranked by peak count
4. **Spike Details** - per-window root cause analysis for each spike:
   - Worst individual delay in the window (latency spikes)
   - Burst breakdown: how many goroutines became runnable, by category
     (unblocked, new, preempted, syscall)
   - Heavy unblockers: which goroutines unblocked the most others
   - Longest run during the wait (latency spikes)
   - Queue activity (latency spikes)

### Example

```
--- Latency Spikes (p99 > 1ms per 100ms) ---
1 window(s) above threshold

  [1] t=4200ms  p99=6.85ms  max=9.44ms  12933 events

--- Runnable Spikes (>80 runnable per 100ms) ---
100 window(s) above threshold (showing top 5)

  [2] t=4200ms  peak 542 runnable
  [3] t=9800ms  peak 237 runnable
  ...

--- Spike Details ---

[1] t=4200ms [latency] p99=6.85ms
   → G852 waited 9.44ms on P6
   → Burst: 125 goroutines became runnable within ±1ms
     Breakdown: 139 unblocked
     Unblocked by (94): selectgo
     Heavy unblocker G841 (4): (*writeBatch).CommitNoSyncWait → ...
   → Longest run during wait: G901 ran 886.8µs
   → Queue activity: 174 goroutines ran 176 times during the wait

[2] t=4200ms [runnable] peak 542 runnable
     Breakdown: 1214 unblocked, 229 preempted, 1 new
     Unblocked by (832): (*Cond).Signal
     Heavy unblocker G1222 (389): (*Store).HandleRaftRequest → ...
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-w`, `--window` | `100ms` | Time window for analysis |
| `--spike-threshold` | `1ms` | p99 threshold to flag as latency spike |
| `--runnable-threshold` | `0` | Runnable goroutine count threshold (0 = 5*GOMAXPROCS) |
| `-n`, `--top` | `5` | Number of spike listings and detail entries |
| `--timeseries` | `false` | Show p99 latency per time window |
| `--by-creator` | `false` | Group delays by goroutine creator |
| `--gc` | `false` | Show GC-related state transitions |
| `--bursts` | `false` | Show burst events and who launched delayed goroutines |
| `--worst` | `0` | Show N worst individual delays with stacks |
| `--top-waiters` | `false` | Show goroutines with most total wait time |
| `--keep-db` | `false` | Keep DuckDB file after analysis |
| `--sql` | `false` | Drop into DuckDB shell after analysis |
| `-v`, `--verbose` | `false` | Verbose output |

## Collecting a trace

```bash
curl -o trace.out 'http://localhost:8080/debug/pprof/trace?seconds=10'
```

Or programmatically:

```go
f, _ := os.Create("trace.out")
trace.Start(f)
defer trace.Stop()
```
