# schedstat - Go Execution Trace Analyzer

## Goal

Analyze Go execution traces and answer: **"What's causing goroutines to wait?"**

The tool should be useful out-of-the-box for someone taking a first look, while also
providing flexibility for power users to dig deeper.

## Current Implementation (v0.1)

### Architecture

```
trace.out → sqlprof → DuckDB → schedstat queries → formatted output
```

- **sqlprof**: Felix Geisendörfer's tool converts traces to DuckDB
- **schedstat**: Runs analysis queries and presents results

### CLI Interface

```bash
# Basic analysis (summary + anomalies + top goroutines)
schedstat trace.out

# Root cause analysis (recommended)
schedstat --why=5 trace.out          # Explain 5 worst delays: what caused them?
schedstat --bursts trace.out         # Detect goroutine launch bursts + who launched them

# Additional analyses
schedstat --worst=20 trace.out       # N worst individual delays with stacks
schedstat --timeseries trace.out     # p99 per time window
schedstat --by-creator trace.out     # delays grouped by goroutine creator
schedstat --gc trace.out             # GC-related state transitions

# Power user
schedstat --sql trace.out            # drop into DuckDB shell after analysis
schedstat --keep-db trace.out        # keep .duckdb file for later exploration
```

### Stack Display

Stacks are shown as meaningful call paths instead of just the terminal frame:
```
(*Stopper).RunAsyncTaskEx.func2 → (*DistSender).sendPartialBatchAsync.func1 → ...
```
The path shows: entry point → key intermediate frames → terminal frame.
Runtime/stdlib frames are filtered out to focus on application code.

### Default Output

1. **Trace duration** - orientation
2. **Overall latency stats** - min, p50, p90, p99, max
3. **Anomalies** - time windows where p99 exceeded threshold
4. **Top goroutines** - which goroutines spent the most time waiting

### Key Design Decisions

- **Show data, not conclusions**: Present observations, let users draw inferences
- **Wide view first, then drill-down**: Summary → anomalies → details
- **Power user escape hatch**: `--sql` drops into DuckDB for arbitrary queries
- **Build on sqlprof**: Don't reinvent trace parsing; leverage existing work

---

## Future: Web App (Fully Client-Side)

Goal: A hosted static page where users can drop a trace file and get instant analysis.
No CLI, no terminal, no server-side processing.

```
User bookmarks: schedstat.example.com
User drops:     trace.out
Browser does:   Go WASM parses → DuckDB-WASM queries → React UI displays
```

### Feasibility (Validated Feb 2026)

**Go WASM compilation: WORKS**

Tested compiling `golang.org/x/exp/trace` to WASM - succeeded on first try.

| Metric | Value |
|--------|-------|
| Compilation | ✓ No issues |
| Raw WASM size | 3.6 MB |
| Gzipped size | ~1 MB |
| Integration effort | 1-2 days |

Proof-of-concept at `experiments/go-wasm/`:
- `main.go` - WASM source using `golang.org/x/exp/trace`
- `trace-parser.wasm` - Compiled binary (3.6 MB)
- Exports `parseGoTrace(Uint8Array)` → JSON events

**TypeScript parser alternative:**

| Aspect | Assessment |
|--------|------------|
| Format | LEB128 encoding, ~44 event types |
| Subset needed | ~10 event types for scheduling analysis |
| Effort | 2-3 days MVP, 1 week production |
| Maintenance | Must track Go version changes |

Not recommended - Go WASM gives automatic compatibility with all trace versions.

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Browser                                                │
│                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────┐ │
│  │ Go WASM      │    │ DuckDB-WASM  │    │ React UI  │ │
│  │ (1MB gzip)   │ →  │ (queries)    │ →  │ (display) │ │
│  │              │    │              │    │           │ │
│  │ trace.out    │    │ g_transitions│    │ tables    │ │
│  │ → events JSON│    │ stacks, etc  │    │ charts    │ │
│  └──────────────┘    └──────────────┘    └───────────┘ │
└─────────────────────────────────────────────────────────┘
```

**Data flow:**
1. User drops `trace.out`
2. Go WASM parses trace → JSON array of scheduling events
3. DuckDB-WASM loads JSON into tables
4. Same SQL queries from CLI run in browser
5. React renders results with interactive tables/charts

### Size Mitigations

The 1MB gzipped WASM is acceptable for a specialized tool, but can optimize:
- Lazy load WASM only after user drops a file
- Use streaming WASM instantiation
- Cache in browser IndexedDB/Cache API
- Consider code splitting for DuckDB-WASM

### Web UI Stack

- **Framework**: React + TypeScript + Vite
- **Data**: DuckDB-WASM + React Query for caching
- **Tables**: TanStack Table (sorting, filtering, pagination)
- **Timeline**: D3.js or patternfly-timeline
- **Charts**: Recharts or nivo
- **Styling**: Tailwind CSS

### UI Features

MVP:
- Drag & drop trace file
- Summary statistics (duration, event count, percentiles)
- Anomaly windows table (sortable by p99, time)
- Top goroutines table (sortable by total wait, max wait)
- SQL console for power users

Future:
- Timeline visualization (swimlanes by goroutine/processor)
- Click anomaly → drill into that time window
- Flame graph for stack traces
- Runnable count over time chart

### Existing Tools to Learn From

| Tool | Approach | Relevant Features |
|------|----------|-------------------|
| [gotraceui](https://gotraceui.dev/) | Native (Go + Gio) | Timeline design, flame graphs |
| [Perfetto UI](https://ui.perfetto.dev) | Web | Drag-drop, WASD nav, drill-down |
| Chrome DevTools Performance | Web | Timeline + call tree |
| pprof web | Go server | Flame graph, top-N tables |

### Alternative: Interactive CLI (TUI)

Lower priority, but could add `--tui` mode using bubbletea:
- Summary view, timeseries view, table view, SQL shell
- Stays in terminal, no browser context switch
- ASCII charts are limited but functional

---

## Analyses to Add

Based on real-world usage (e.g., Tobi's investigation):

### Already Implemented
- [x] Overall latency percentiles
- [x] Latency per time window (`--timeseries`)
- [x] Anomaly detection (spike windows)
- [x] Top goroutines by wait time (with meaningful stack paths)
- [x] Delays by creator (`--by-creator`)
- [x] GC-related transitions (`--gc`)
- [x] Worst individual delays (`--worst=N`) with full stack paths
- [x] **Root cause analysis** (`--why=N`) - For each delay, show what was running on the P
      and whether it was a single blocker or runqueue saturation
- [x] **Burst detection** (`--bursts`) - When many goroutines became runnable at once
- [x] **Who launched delayed goroutines** - What code path created goroutines that experienced delays

### To Add
- [ ] **Runnable count over time** - How many goroutines were runnable at each point?
      This is key for diagnosing "too many goroutines" situations.
- [ ] **Processor utilization** - Were all Ps busy? Were some idle while work waited?
- [ ] **Correlation with metrics** - Overlay scheduling latency with /gc/heap/allocs, etc.
- [ ] **STW detection** - Identify stop-the-world pauses and their duration

### CRDB-Specific (Optional)
- [ ] Recognize DistSender patterns
- [ ] Recognize SQL execution patterns
- [ ] Suggest relevant tuning knobs (e.g., concurrency limits)

---

## Dependencies

- `github.com/felixge/sqlprof` - trace to DuckDB conversion
- `github.com/marcboeker/go-duckdb` - DuckDB Go driver
- (future) `github.com/charmbracelet/bubbletea` - TUI framework

---

## File Structure

### Current
```
schedstat/
├── main.go              # CLI entry point, all analysis logic
├── docs/
│   └── PLAN.md          # This file
├── experiments/
│   ├── ASSESSMENT.md    # WASM feasibility findings
│   └── go-wasm/         # Proof-of-concept WASM parser
│       ├── main.go      # WASM source (golang.org/x/exp/trace)
│       ├── trace-parser.wasm  # Compiled binary (3.6 MB)
│       └── README.md
├── testdata/
│   └── gentrace/        # Test trace generator
│       └── main.go
├── go.mod
└── go.sum
```

### Future (with web app)
```
schedstat/
├── main.go              # CLI
├── docs/
├── experiments/
├── testdata/
├── web/                 # Static web app
│   ├── index.html
│   ├── src/
│   │   ├── main.tsx
│   │   ├── components/
│   │   │   ├── DropZone.tsx
│   │   │   ├── Summary.tsx
│   │   │   ├── AnomalyTable.tsx
│   │   │   ├── GoroutineTable.tsx
│   │   │   └── SqlConsole.tsx
│   │   ├── hooks/
│   │   │   └── useDuckDB.ts
│   │   ├── wasm/
│   │   │   └── trace-parser.wasm
│   │   └── queries/
│   │       └── schedstat.ts  # SQL queries (mirror main.go)
│   ├── package.json
│   └── vite.config.ts
├── go.mod
└── go.sum
```

---

## Testing

Need traces from:
1. Simple test programs (already have `testdata/gentrace`)
2. Real CRDB workloads with known latency issues
3. Traces with GC pressure
4. Traces with goroutine explosion (like the DistSender case)

Note: sqlprof currently doesn't support Go 1.25 traces. Need to use
Go 1.24 or wait for sqlprof update.
