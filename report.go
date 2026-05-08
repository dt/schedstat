package main

import (
	"fmt"
	"strings"
)

// Report aggregates the output of a single trace analysis. Its String method
// reproduces the plaintext output; json.Marshal produces the JSON form. All
// duration fields are exposed as raw nanoseconds.
type Report struct {
	TraceFile      string               `json:"trace_file"`
	DurationMs     float64              `json:"duration_ms"`
	Overall        *OverallStats        `json:"overall,omitempty"`
	Timeseries     *TimeseriesReport    `json:"timeseries,omitempty"`
	LatencySpikes  *SpikesReport        `json:"latency_spikes,omitempty"`
	RunnableSpikes *SpikesReport        `json:"runnable_spikes,omitempty"`
	SpikeDetails   *SpikeDetailsSection `json:"spike_details,omitempty"`
	ByCreator      *ByCreatorReport     `json:"by_creator,omitempty"`
	GC             *GCReport            `json:"gc,omitempty"`
	Bursts         *BurstReport         `json:"bursts,omitempty"`
	WorstDelays    *WorstDelaysReport   `json:"worst_delays,omitempty"`
	TopGoroutines  *TopGoroutinesReport `json:"top_goroutines,omitempty"`
}

// Header returns the trace-identification header (filename + duration).
func (r *Report) Header() string {
	var b strings.Builder
	fmt.Fprintf(&b, "schedstat: %s\n", r.TraceFile)
	b.WriteString(strings.Repeat("=", 60))
	b.WriteByte('\n')
	fmt.Fprintf(&b, "\nTrace duration: %.1fms\n", r.DurationMs)
	return b.String()
}

// String renders the full report as plaintext, in the same order and format as
// the legacy streaming output. Disabled sections are omitted.
func (r *Report) String() string {
	var b strings.Builder
	b.WriteString(r.Header())
	emit := func(s fmt.Stringer) {
		if s != nil {
			b.WriteString(s.String())
		}
	}
	if r.Overall != nil {
		emit(r.Overall)
	}
	if r.Timeseries != nil {
		emit(r.Timeseries)
	}
	if r.LatencySpikes != nil {
		emit(r.LatencySpikes)
	}
	if r.RunnableSpikes != nil {
		emit(r.RunnableSpikes)
	}
	if r.SpikeDetails != nil {
		emit(r.SpikeDetails)
	}
	if r.ByCreator != nil {
		emit(r.ByCreator)
	}
	if r.GC != nil {
		emit(r.GC)
	}
	if r.Bursts != nil {
		emit(r.Bursts)
	}
	if r.WorstDelays != nil {
		emit(r.WorstDelays)
	}
	if r.TopGoroutines != nil {
		emit(r.TopGoroutines)
	}
	return b.String()
}

// fmtNsPtr formats a nullable nanosecond duration; nil renders as "-".
func fmtNsPtr(p *int64) string {
	if p == nil {
		return "-"
	}
	return fmtDuration(float64(*p))
}

// --- Overall stats ---

type OverallStats struct {
	Count int    `json:"count"`
	MinNs *int64 `json:"min_ns,omitempty"`
	AvgNs *int64 `json:"avg_ns,omitempty"`
	P50Ns *int64 `json:"p50_ns,omitempty"`
	P90Ns *int64 `json:"p90_ns,omitempty"`
	P99Ns *int64 `json:"p99_ns,omitempty"`
	MaxNs *int64 `json:"max_ns,omitempty"`
}

func (o *OverallStats) String() string {
	var b strings.Builder
	b.WriteString("\n--- Scheduling Latency (runnable → running) ---\n")
	fmt.Fprintf(&b, "Events: %d\n", o.Count)
	if o.Count > 0 {
		fmt.Fprintf(&b, "  min: %-10s  p50: %-10s  p90: %-10s\n",
			fmtNsPtr(o.MinNs), fmtNsPtr(o.P50Ns), fmtNsPtr(o.P90Ns))
		fmt.Fprintf(&b, "  avg: %-10s  p99: %-10s  max: %-10s\n",
			fmtNsPtr(o.AvgNs), fmtNsPtr(o.P99Ns), fmtNsPtr(o.MaxNs))
	}
	return b.String()
}

// --- Timeseries ---

type TimeseriesReport struct {
	WindowLabel string             `json:"window_label"`
	ThresholdNs int64              `json:"threshold_ns"`
	Windows     []TimeseriesWindow `json:"windows"`
}

type TimeseriesWindow struct {
	Window  int     `json:"window"`
	StartMs float64 `json:"start_ms"`
	EndMs   float64 `json:"end_ms"`
	Events  int     `json:"events"`
	P99Ns   float64 `json:"p99_ns"`
}

func (t *TimeseriesReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n--- Latency by %s Window ---\n", t.WindowLabel)
	fmt.Fprintf(&b, "%-8s %-10s %-10s %-8s %-12s\n", "Window", "Start(ms)", "End(ms)", "Events", "p99")
	fmt.Fprintf(&b, "%-8s %-10s %-10s %-8s %-12s\n", "------", "---------", "-------", "------", "---")
	for _, w := range t.Windows {
		marker := ""
		if int64(w.P99Ns) > t.ThresholdNs {
			marker = " ←"
		}
		fmt.Fprintf(&b, "%-8d %-10.0f %-10.0f %-8d %-12s%s\n",
			w.Window, w.StartMs, w.EndMs, w.Events, fmtDuration(w.P99Ns), marker)
	}
	return b.String()
}

// --- Spike summaries ---

// SpikesReport captures a Latency or Runnable spike summary section.
type SpikesReport struct {
	Kind              string         `json:"kind"` // "latency" or "runnable"
	WindowLabel       string         `json:"window_label"`
	LatencyThreshold  string         `json:"latency_threshold,omitempty"`
	RunnableThreshold int            `json:"runnable_threshold,omitempty"`
	Total             int            `json:"total"`
	Spikes            []SpikeSummary `json:"spikes"`
}

// SpikeSummary identifies one spike window. Some fields are populated only for
// one Kind; the omitempty tags drop the rest in JSON.
type SpikeSummary struct {
	// Type discriminates "latency" vs "runnable". Not serialized: the
	// containing SpikesReport already encodes kind, and collectSpikeDetails
	// consumes it locally.
	Type         string  `json:"-"`
	Index        int     `json:"index"`
	WindowNum    int     `json:"window_num"`
	StartMs      float64 `json:"start_ms"`
	EventCount   int     `json:"event_count,omitempty"`
	P99Ns        float64 `json:"p99_ns,omitempty"`
	MaxLatencyNs float64 `json:"max_latency_ns,omitempty"`
	MaxRunnable  int     `json:"max_runnable,omitempty"`
}

func (s *SpikesReport) String() string {
	if s.Total == 0 {
		return ""
	}
	var b strings.Builder
	if s.Kind == "latency" {
		fmt.Fprintf(&b, "\n--- Latency Spikes (p99 > %s per %s) ---\n", s.LatencyThreshold, s.WindowLabel)
	} else {
		fmt.Fprintf(&b, "\n--- Runnable Spikes (>%d runnable per %s) ---\n", s.RunnableThreshold, s.WindowLabel)
	}
	if s.Total > len(s.Spikes) {
		fmt.Fprintf(&b, "%d window(s) above threshold (showing top %d)\n", s.Total, len(s.Spikes))
	} else {
		fmt.Fprintf(&b, "%d window(s) above threshold\n", s.Total)
	}
	b.WriteByte('\n')
	for _, sp := range s.Spikes {
		if s.Kind == "latency" {
			fmt.Fprintf(&b, "  [%d] t=%.0fms  p99=%s  max=%s  %d events\n",
				sp.Index, sp.StartMs, fmtDuration(sp.P99Ns), fmtDuration(sp.MaxLatencyNs), sp.EventCount)
		} else {
			fmt.Fprintf(&b, "  [%d] t=%.0fms  peak %d runnable\n",
				sp.Index, sp.StartMs, sp.MaxRunnable)
		}
	}
	return b.String()
}

// --- Spike details ---

type SpikeDetailsSection struct {
	Details []SpikeDetail `json:"details"`
}

func (s *SpikeDetailsSection) String() string {
	var b strings.Builder
	b.WriteString("\n--- Spike Details ---\n")
	for _, d := range s.Details {
		b.WriteByte('\n')
		b.WriteString(d.String())
	}
	return b.String()
}

// SpikeDetail is the per-spike findings block. Type-specific fields use
// pointers to distinguish "absent" from "zero".
type SpikeDetail struct {
	Index       int     `json:"index"`
	Type        string  `json:"type"`
	StartMs     float64 `json:"start_ms"`
	P99Ns       float64 `json:"p99_ns,omitempty"`
	MaxRunnable int     `json:"max_runnable,omitempty"`

	// Latency-spike findings (nil ⇒ "no rows" / not applicable).
	WorstG          *int64       `json:"worst_g,omitempty"`
	WorstDurationNs *int64       `json:"worst_duration_ns,omitempty"`
	WorstP          *int64       `json:"worst_p,omitempty"`
	BurstCount      *int         `json:"burst_count,omitempty"`
	LongestRun      *LongestRun  `json:"longest_run,omitempty"`
	QueueRunners    *int         `json:"queue_runners,omitempty"`
	QueueRuns       *int         `json:"queue_runs,omitempty"`

	// Burst breakdown (latency uses only when burst threshold met; runnable
	// always populates).
	BurstBreakdown *BurstBreakdown `json:"burst_breakdown,omitempty"`
}

type LongestRun struct {
	G          int64    `json:"g"`
	DurationNs int64    `json:"duration_ns"`
	Stack      []string `json:"stack,omitempty"`
}

func (d *SpikeDetail) String() string {
	var b strings.Builder
	if d.Type == "latency" {
		fmt.Fprintf(&b, "[%d] t=%.0fms [latency] p99=%s\n", d.Index, d.StartMs, fmtDuration(d.P99Ns))
		if d.WorstG == nil {
			return b.String()
		}
		fmt.Fprintf(&b, "   → g%d waited %s", *d.WorstG, fmtDuration(float64(*d.WorstDurationNs)))
		if d.WorstP != nil {
			fmt.Fprintf(&b, " on P%d", *d.WorstP)
		}
		b.WriteByte('\n')

		burstFired := d.BurstCount != nil && *d.BurstCount > 10
		longFired := d.LongestRun != nil
		queueFired := d.QueueRunners != nil && *d.QueueRunners > 0

		if burstFired {
			fmt.Fprintf(&b, "   → Burst: %d goroutines became runnable within ±1ms\n", *d.BurstCount)
			if d.BurstBreakdown != nil {
				b.WriteString(d.BurstBreakdown.String())
			}
		}
		if longFired {
			fmt.Fprintf(&b, "   → Longest run during wait: g%d ran %s\n",
				d.LongestRun.G, fmtDuration(float64(d.LongestRun.DurationNs)))
			if len(d.LongestRun.Stack) > 0 {
				fmt.Fprintf(&b, "     %s\n", formatStack(d.LongestRun.Stack, 80))
			}
		}
		if queueFired {
			fmt.Fprintf(&b, "   → Queue activity: %d goroutines ran %d times during the wait\n",
				*d.QueueRunners, *d.QueueRuns)
		}
		if !burstFired && !longFired && !queueFired {
			b.WriteString("   → No clear single cause identified\n")
		}
	} else { // runnable
		fmt.Fprintf(&b, "[%d] t=%.0fms [runnable] peak %d runnable\n", d.Index, d.StartMs, d.MaxRunnable)
		if d.BurstBreakdown != nil {
			b.WriteString(d.BurstBreakdown.String())
		}
	}
	return b.String()
}

// BurstBreakdown captures the categorized breakdown plus optional sub-lists for
// the "new" and "unblocked" categories.
type BurstBreakdown struct {
	Categories      []BurstCategory       `json:"categories"`
	Creators        []CreatorEntry        `json:"creators,omitempty"`
	Unblockers      []UnblockerEntry      `json:"unblockers,omitempty"`
	BlockedAt       []BlockedAtEntry      `json:"blocked_at,omitempty"`
	HeavyUnblockers []HeavyUnblockerEntry `json:"heavy_unblockers,omitempty"`

	// CreatorsKnown is true iff at least one creator row was returned. Needed
	// to reproduce the "(no creator info available)" line for the new branch
	// when zero rows came back.
	CreatorsKnown bool `json:"-"`
}

type BurstCategory struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type CreatorEntry struct {
	Count int      `json:"count"`
	Stack []string `json:"stack,omitempty"`
}

type UnblockerEntry struct {
	Count int    `json:"count"`
	Func  string `json:"func"`
}

type BlockedAtEntry struct {
	Count int      `json:"count"`
	Stack []string `json:"stack"`
	// Leaf is shown in brackets when the leaf frame is filtered out by
	// formatStack (runtime/sync), to keep entries distinguishable.
	LeafBracket string `json:"leaf_bracket,omitempty"`
}

type HeavyUnblockerEntry struct {
	G     int64    `json:"g"`
	Count int      `json:"count"`
	Stack []string `json:"stack,omitempty"`
}

func (b *BurstBreakdown) String() string {
	var sb strings.Builder
	parts := make([]string, 0, len(b.Categories))
	for _, c := range b.Categories {
		parts = append(parts, fmt.Sprintf("%d %s", c.Count, c.Name))
	}
	fmt.Fprintf(&sb, "     Breakdown: %s\n", strings.Join(parts, ", "))

	for _, c := range b.Categories {
		switch c.Name {
		case "new":
			if c.Count == 0 {
				continue
			}
			if !b.CreatorsKnown {
				sb.WriteString("     (no creator info available)\n")
				continue
			}
			for _, ce := range b.Creators {
				if len(ce.Stack) == 0 {
					fmt.Fprintf(&sb, "     Created by (%d): (unknown creator)\n", ce.Count)
				} else {
					fmt.Fprintf(&sb, "     Created by (%d): %s\n", ce.Count, formatStack(ce.Stack, 90))
				}
			}
		case "unblocked":
			if c.Count == 0 {
				continue
			}
			for _, u := range b.Unblockers {
				if u.Func == "" {
					fmt.Fprintf(&sb, "     Unblocked by (%d): (unknown)\n", u.Count)
				} else {
					fmt.Fprintf(&sb, "     Unblocked by (%d): %s\n", u.Count, u.Func)
				}
			}
			for _, ba := range b.BlockedAt {
				if ba.LeafBracket != "" {
					fmt.Fprintf(&sb, "     Blocked at (%d): %s [%s]\n", ba.Count, formatStack(ba.Stack, 80), ba.LeafBracket)
				} else {
					fmt.Fprintf(&sb, "     Blocked at (%d): %s\n", ba.Count, formatStack(ba.Stack, 80))
				}
			}
			for _, hu := range b.HeavyUnblockers {
				if len(hu.Stack) == 0 {
					fmt.Fprintf(&sb, "     Heavy unblocker g%d (%d): (unknown stack)\n", hu.G, hu.Count)
				} else {
					fmt.Fprintf(&sb, "     Heavy unblocker g%d (%d): %s\n", hu.G, hu.Count, formatStack(hu.Stack, 100))
				}
			}
		}
	}
	return sb.String()
}

// --- By creator ---

type ByCreatorReport struct {
	Rows []CreatorRow `json:"rows"`
}

type CreatorRow struct {
	Creator    string `json:"creator"`
	Count      int    `json:"count"`
	TotalNs    int64  `json:"total_ns"`
	MaxNs      int64  `json:"max_ns"`
	P99Ns      int64  `json:"p99_ns"`
}

func (r *ByCreatorReport) String() string {
	var b strings.Builder
	b.WriteString("\n--- Delays by Goroutine Creator ---\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "  %s\n", shortenFunc(row.Creator))
		fmt.Fprintf(&b, "    %d events, total: %s, max: %s, p99: %s\n",
			row.Count, fmtDuration(float64(row.TotalNs)), fmtDuration(float64(row.MaxNs)), fmtDuration(float64(row.P99Ns)))
	}
	return b.String()
}

// --- GC analysis ---

type GCReport struct {
	CycleSummary      *GCCycleSummary       `json:"cycle_summary,omitempty"`
	STW               *GCSTWSummary         `json:"stw,omitempty"`
	MarkAssist        *GCMarkAssist         `json:"mark_assist,omitempty"`
	LatencyComparison *GCLatencyComparison  `json:"latency_comparison,omitempty"`
	PerCycle          *GCPerCycleBreakdown  `json:"per_cycle,omitempty"`
	Sweep             *GCSweepSummary       `json:"sweep,omitempty"`
}

func (g *GCReport) String() string {
	var b strings.Builder
	b.WriteString("\n--- GC Analysis ---\n")
	if g.CycleSummary != nil {
		b.WriteString(g.CycleSummary.String())
	}
	if g.STW != nil {
		b.WriteString(g.STW.String())
	}
	if g.MarkAssist != nil {
		b.WriteString(g.MarkAssist.String())
	}
	if g.LatencyComparison != nil {
		b.WriteString(g.LatencyComparison.String())
	}
	if g.PerCycle != nil {
		b.WriteString(g.PerCycle.String())
	}
	if g.Sweep != nil {
		b.WriteString(g.Sweep.String())
	}
	return b.String()
}

type GCCycleSummary struct {
	Count   int    `json:"count"`
	TotalNs *int64 `json:"total_ns,omitempty"`
	AvgNs   *int64 `json:"avg_ns,omitempty"`
	MinNs   *int64 `json:"min_ns,omitempty"`
	MaxNs   *int64 `json:"max_ns,omitempty"`
}

func (g *GCCycleSummary) String() string {
	if g.Count == 0 {
		return "GC cycles: 0\n"
	}
	return fmt.Sprintf("GC cycles: %d, total: %s, avg: %s, min: %s, max: %s\n",
		g.Count, fmtNsPtr(g.TotalNs), fmtNsPtr(g.AvgNs), fmtNsPtr(g.MinNs), fmtNsPtr(g.MaxNs))
}

type GCSTWSummary struct {
	TotalCount int        `json:"total_count"`
	TotalNs    int64      `json:"total_ns"`
	MaxNs      int64      `json:"max_ns"`
	Reasons    []STWEntry `json:"reasons"`
}

type STWEntry struct {
	Reason  string `json:"reason"`
	Count   int    `json:"count"`
	TotalNs int64  `json:"total_ns"`
	MaxNs   int64  `json:"max_ns"`
}

func (g *GCSTWSummary) String() string {
	if g.TotalCount == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  STW pauses: %d, total: %s, max: %s\n",
		g.TotalCount, fmtDuration(float64(g.TotalNs)), fmtDuration(float64(g.MaxNs)))
	for _, e := range g.Reasons {
		fmt.Fprintf(&b, "    %s: %d pauses, total %s, max %s\n",
			e.Reason, e.Count, fmtDuration(float64(e.TotalNs)), fmtDuration(float64(e.MaxNs)))
	}
	return b.String()
}

type GCMarkAssist struct {
	TotalEvents     int                 `json:"total_events"`
	TotalGoroutines int                 `json:"total_goroutines"`
	TotalNs         *int64              `json:"total_ns,omitempty"`
	MaxSingleNs     *int64              `json:"max_single_ns,omitempty"`
	TopGoroutines   []MarkAssistGEntry  `json:"top_goroutines,omitempty"`
}

type MarkAssistGEntry struct {
	G            int64   `json:"g"`
	Name         string  `json:"name"`
	Assists      int     `json:"assists"`
	TotalNs      int64   `json:"total_ns"`
	MaxNs        int64   `json:"max_ns"`
	WorstAtMs    float64 `json:"worst_at_ms"`
}

func (g *GCMarkAssist) String() string {
	if g.TotalEvents == 0 {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  Mark assist: %d events across %d goroutines, total: %s, max single: %s\n",
		g.TotalEvents, g.TotalGoroutines, fmtNsPtr(g.TotalNs), fmtNsPtr(g.MaxSingleNs))
	b.WriteString("    Top affected goroutines:\n")
	for _, e := range g.TopGoroutines {
		fmt.Fprintf(&b, "      g%-8d %-40s %d assists, total %s, max %s @ t=%.0fms\n",
			e.G, shortenFunc(e.Name), e.Assists,
			fmtDuration(float64(e.TotalNs)), fmtDuration(float64(e.MaxNs)), e.WorstAtMs)
	}
	return b.String()
}

type GCLatencyComparison struct {
	DuringGC LatencyBucket `json:"during_gc"`
	NonGC    LatencyBucket `json:"non_gc"`
	// RatioP50 etc: only present when nonGC.p50 > 0 and nonGC.p50 valid and duringGC.p50 valid.
	HasRatio  bool    `json:"-"`
	RatioP50  float64 `json:"ratio_p50,omitempty"`
	RatioP99  float64 `json:"ratio_p99,omitempty"`
	RatioMax  float64 `json:"ratio_max,omitempty"`
}

type LatencyBucket struct {
	Count int    `json:"count"`
	P50Ns *int64 `json:"p50_ns,omitempty"`
	P99Ns *int64 `json:"p99_ns,omitempty"`
	MaxNs *int64 `json:"max_ns,omitempty"`
}

func (g *GCLatencyComparison) String() string {
	if g.DuringGC.Count == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  Scheduling latency during GC vs normal:\n")
	fmt.Fprintf(&b, "    %-14s %-10s %-10s %-10s %-10s\n", "", "count", "p50", "p99", "max")
	fmt.Fprintf(&b, "    %-14s %-10d %-10s %-10s %-10s\n",
		"During GC:", g.DuringGC.Count, fmtNsPtr(g.DuringGC.P50Ns), fmtNsPtr(g.DuringGC.P99Ns), fmtNsPtr(g.DuringGC.MaxNs))
	fmt.Fprintf(&b, "    %-14s %-10d %-10s %-10s %-10s\n",
		"Non-GC:", g.NonGC.Count, fmtNsPtr(g.NonGC.P50Ns), fmtNsPtr(g.NonGC.P99Ns), fmtNsPtr(g.NonGC.MaxNs))
	if g.HasRatio {
		fmt.Fprintf(&b, "    %-14s %-10s %-10s %-10s %-10s\n",
			"Ratio:", "", fmt.Sprintf("%.1fx", g.RatioP50), fmt.Sprintf("%.1fx", g.RatioP99), fmt.Sprintf("%.1fx", g.RatioMax))
	}
	return b.String()
}

type GCPerCycleBreakdown struct {
	Cycles []GCCycleEntry `json:"cycles"`
}

type GCCycleEntry struct {
	Cycle        int   `json:"cycle"`
	DurationNs   int64 `json:"duration_ns"`
	Assists      int   `json:"assists"`
	Goroutines   int   `json:"goroutines"`
	AssistTimeNs int64 `json:"assist_time_ns"`
}

func (g *GCPerCycleBreakdown) String() string {
	var b strings.Builder
	b.WriteString("  Per-cycle breakdown:\n")
	fmt.Fprintf(&b, "    %-6s %-12s %-8s %-12s %-12s\n", "Cycle", "Duration", "Assists", "Goroutines", "Assist Time")
	for _, c := range g.Cycles {
		fmt.Fprintf(&b, "    %-6d %-12s %-8d %-12d %-12s\n",
			c.Cycle, fmtDuration(float64(c.DurationNs)), c.Assists, c.Goroutines, fmtDuration(float64(c.AssistTimeNs)))
	}
	return b.String()
}

type GCSweepSummary struct {
	Count   int    `json:"count"`
	TotalNs *int64 `json:"total_ns,omitempty"`
}

func (g *GCSweepSummary) String() string {
	if g.Count == 0 {
		return ""
	}
	return fmt.Sprintf("  Sweep: %d events, total: %s\n", g.Count, fmtNsPtr(g.TotalNs))
}

// --- Bursts ---

type BurstReport struct {
	Bursts          []BurstWindow    `json:"bursts"`
	DelayedCreators []DelayedCreator `json:"delayed_creators"`
	HasDelayed      bool             `json:"-"`
}

type BurstWindow struct {
	StartMs           float64 `json:"start_ms"`
	GoroutinesSpawned int     `json:"goroutines_spawned"`
	DistinctCreators  int     `json:"distinct_creators"`
}

type DelayedCreator struct {
	Stack        []string `json:"stack"`
	DelayedCount int      `json:"delayed_count"`
	TotalDelayNs int64    `json:"total_delay_ns"`
	MaxDelayNs   int64    `json:"max_delay_ns"`
}

func (br *BurstReport) String() string {
	var b strings.Builder
	b.WriteString("\n--- Goroutine Bursts (>10 becoming runnable in 1ms) ---\n")
	if len(br.Bursts) == 0 {
		b.WriteString("  No significant bursts detected.\n")
	}
	for _, w := range br.Bursts {
		fmt.Fprintf(&b, "  t=%-8.1fms: %d goroutines became runnable (%d distinct creators)\n",
			w.StartMs, w.GoroutinesSpawned, w.DistinctCreators)
	}
	b.WriteString("\n--- Who Launched Delayed Goroutines? ---\n")
	if !br.HasDelayed {
		b.WriteString("  (no creator info available)\n")
	}
	for _, dc := range br.DelayedCreators {
		fmt.Fprintf(&b, "  %s\n", formatStack(dc.Stack, 80))
		fmt.Fprintf(&b, "    launched %d delayed goroutines, total delay: %s, max: %s\n",
			dc.DelayedCount, fmtDuration(float64(dc.TotalDelayNs)), fmtDuration(float64(dc.MaxDelayNs)))
	}
	return b.String()
}

// --- Worst delays ---

type WorstDelaysReport struct {
	N    int              `json:"n"`
	Rows []WorstDelayRow  `json:"rows"`
}

type WorstDelayRow struct {
	Rank       int      `json:"rank"`
	G          int64    `json:"g"`
	DurationNs int64    `json:"duration_ns"`
	Stack      []string `json:"stack,omitempty"`
}

func (w *WorstDelaysReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n--- %d Worst Individual Delays ---\n", w.N)
	for _, r := range w.Rows {
		fmt.Fprintf(&b, "\n%d. g%d waited %s\n", r.Rank, r.G, fmtDuration(float64(r.DurationNs)))
		fmt.Fprintf(&b, "   %s\n", formatStack(r.Stack, 100))
	}
	return b.String()
}

// --- Top goroutines ---

type TopGoroutinesReport struct {
	Rows []TopGoroutineRow `json:"rows"`
}

type TopGoroutineRow struct {
	G          int64    `json:"g"`
	Stack      []string `json:"stack,omitempty"`
	SchedCount int      `json:"sched_count"`
	TotalNs    int64    `json:"total_ns"`
	MaxNs      int64    `json:"max_ns"`
}

func (t *TopGoroutinesReport) String() string {
	var b strings.Builder
	b.WriteString("\n--- Top Goroutines by Wait Time ---\n")
	for _, r := range t.Rows {
		fmt.Fprintf(&b, "  g%-6d %s\n", r.G, formatStack(r.Stack, 80))
		fmt.Fprintf(&b, "          %d schedulings, total: %s, max: %s\n",
			r.SchedCount, fmtDuration(float64(r.TotalNs)), fmtDuration(float64(r.MaxNs)))
	}
	return b.String()
}
