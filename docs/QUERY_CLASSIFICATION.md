# Query Classification

## Overview

The pipeline classifies every Redshift query into one of five categories based on its dominant performance bottleneck. This drives the dashboard's monitoring, alerting, and remediation recommendations.

Thresholds were derived from a behavioral analysis of the full REDSET dataset (433 million queries). Rather than picking arbitrary cutoffs, we profiled queries across metric buckets and identified where behavior measurably shifts — spill rates doubling, execution times crossing multiples of the population average, etc. Each threshold corresponds to one of these transition points.

---

## Rules

Evaluation order matters — first match wins.

```sql
CASE
    WHEN was_cached = 0
         AND query_type IN ('copy', 'unload')
         AND execution_duration_ms > 500
    THEN 'Network-bound'

    WHEN was_cached = 0
         AND query_type NOT IN ('copy', 'unload')
         AND (num_joins > 1 OR num_aggregations > 1)
         AND mbytes_scanned > 0
         AND (execution_duration_ms / (mbytes_scanned + 1)) > 20.0
         AND execution_duration_ms > 1000
    THEN 'CPU-bound'

    WHEN was_cached = 0
         AND query_type NOT IN ('copy', 'unload')
         AND mbytes_scanned >= 2000
         AND (execution_duration_ms / (mbytes_scanned + 1)) < 0.061
         AND (mbytes_spilled > 0 OR num_scans > 1)
         AND execution_duration_ms > 1000
    THEN 'IO-bound'

    WHEN was_cached = 0
         AND queue_duration_ms >= execution_duration_ms
         AND queue_duration_ms > 1000
         AND execution_duration_ms > 0
         AND (num_scans + num_joins + num_aggregations) < 4
    THEN 'Queue/WLM-bound'

    ELSE 'Normal'
END
```

COPY/UNLOAD is checked first to separate data-transfer operations before applying compute or IO tests. Cached queries (`was_cached = 1`) are excluded from all bottleneck classes and fall through to Normal.

---

## Thresholds

### Network-bound

**Definition**: COPY/UNLOAD operations that transfer a meaningful amount of data.

**Why 500 ms**: Below this threshold, most COPY operations serve from cache or move negligible data. At ≥ 500 ms, the median scan volume crosses 50 MB — indicating real data movement.

| COPY Duration | Median Scan (MB) |
|---------------|-------------------|
| < 200 ms | 0 |
| 200–500 ms | 46 |
| **≥ 500 ms** | **176** |

UNLOAD queries move significant volumes at almost any duration, so the 500 ms threshold is effectively set by COPY.

---

### CPU-bound

**Definition**: Complex queries where computation dominates execution time — not disk reads, not network, not queuing.

The key metric is the **exec/scan ratio** (`execution_ms / (mbytes_scanned + 1)`): how much time is spent per megabyte read. A high ratio means the query does heavy work per row (joins, aggregations, sorting) relative to scan volume.

**Why ratio > 20**: We bucketed 281 million qualifying queries by their exec/scan ratio and tracked per-bucket average execution time:

| Ratio Bucket | Count | Avg Exec (ms) | vs Population Avg | Spill Rate |
|-------------|-------|---------------|-------------------|------------|
| 5–10 | 14.1M | 5,072 | 1.82× | 4.47% |
| 10–20 | 9.9M | 6,677 | 2.40× | 3.90% |
| **20–50** | **9.1M** | **7,172** | **2.58×** | **3.22%** |
| 50–100 | 3.0M | 12,665 | 4.56× | 4.94% |
| 100–500 | 1.7M | 39,000 | 14.03× | 8.88% |

At ratio ≥ 20, average execution crosses 2× the population average (7,172 ms vs 2,780 ms), while spill rate stays near the baseline of 2.29%. These queries are genuinely slow due to computation, not memory pressure. Above ratio 100, spill rate climbs to ~9%, meaning those extreme queries start hitting memory limits and are no longer purely compute-bound.

**Why exec > 1000 ms**: Without this, a query running 5 ms on 0.1 MB scan gets ratio 50 — clearly not a CPU problem. The floor eliminates these false positives.

**Why joins > 1 or aggs > 1**: Requires actual structural complexity. A simple SELECT with no joins or aggregations isn't doing meaningful computation.

---

### IO-bound

**Definition**: Large-scan queries bottlenecked by disk throughput. Most of their execution time is proportional to data read volume.

**Why scan ≥ 2000 MB**: We bucketed queries by scan volume and tracked spill behavior:

| Scan Bucket (MB) | Count | Spill Rate | Multi-scan % | Avg Spill (MB) |
|------------------|-------|------------|-------------|----------------|
| 500–1000 | 19.7M | 3.81% | 43.0% | 152 |
| 1000–2000 | 17.9M | 4.25% | 42.1% | 182 |
| **2000–5000** | **11.2M** | **7.74%** | **53.3%** | **649** |
| 5000–10000 | 4.9M | 12.14% | 64.3% | 1,938 |

At 2000 MB, spill rate nearly doubles (4.25% → 7.74%), multi-scan crosses 53%, and average spill volume jumps from 182 MB to 649 MB. This is where disk throughput becomes the dominant constraint.

**Why ratio < 0.061**: This is the average P25 of per-bucket exec/scan ratios for queries scanning ≥ 2000 MB. IO-bound queries have very low ratios because almost all execution time is spent reading. This also makes CPU ∩ IO overlap structurally impossible (ratio > 20 and ratio < 0.061 can't both be true).

**Why spilled > 0 or num_scans > 1**: Confirms actual IO pressure — either disk spill or multiple scan steps.

---

### Queue/WLM-bound

**Definition**: Simple queries that spend more time waiting for cluster resources than executing.

**Why queue ≥ exec**: This is the defining condition. If queue time exceeds execution time, the cluster — not the query — is the bottleneck.

**Why queue > 1000 ms**: Filters out sub-second queuing noise.

**Why complexity < 4** (`num_scans + num_joins + num_aggregations`): Even at high queue fractions, average query complexity stays moderate (5–6). The low-complexity filter isolates genuinely simple queries that are fast on their own and are purely waiting — not complex queries that naturally queue because they need many slots.

There is no sharp transition point in the queue regime. Queue-dominance increases gradually as queue time rises. The `queue ≥ exec` condition does the primary work; the 1000 ms floor and complexity cap refine it.

---

### Normal

Everything else. No single bottleneck dominates.

---

## Distribution

On the full 433M-query dataset:

| Class | Count | % | Exec P50 | Avg Scan | Spill Rate | Queue Dom % | Complexity |
|-------|-------|---|----------|----------|------------|-------------|------------|
| Normal | 401.3M | 92.65% | 37 ms | 3.2 GB | 2.0% | 1.1% | 2.5 |
| Network | 18.1M | 4.18% | 1,410 ms | 29.5 GB | 4.4% | 4.5% | 2.7 |
| CPU-bound | 9.6M | 2.23% | 2,981 ms | 307 MB | 5.6% | 6.8% | 4.3 |
| Queue/WLM | 3.0M | 0.69% | 204 ms | 1.9 GB | 5.3% | 100% | 1.5 |
| IO-bound | 1.1M | 0.25% | 4,522 ms | 938 GB | 27.1% | 3.4% | 7.3 |

Each class shows the expected profile:
- **CPU-bound**: highest ratio (42.6), highest complexity (4.3), moderate scan (307 MB)
- **IO-bound**: highest spill (27.1%), largest scans (938 GB avg), lowest ratio (0.033)
- **Queue/WLM**: 100% queue dominance, lowest complexity (1.5), fast execution (204 ms P50)
- **Network**: significant data movement (29.5 GB avg scan)

---

## Validation

### Purity

Fraction of classified queries that satisfy all expected behavioral criteria:

| Class | Purity | Notes |
|-------|--------|-------|
| CPU-bound | 88.0% | 12% have some spill alongside high compute — borderline cases |
| IO-bound | 100% | |
| Network | 100% | |
| Queue/WLM | 100% | |

### Overlap

Evaluated independently (ignoring CASE priority):

- **CPU ∩ IO = 0** — impossible (contradictory ratio thresholds)
- **CPU ∩ Queue = 652K** — complex slow queries that also queued; CASE assigns to CPU
- **IO ∩ Queue = 37K** — large scans that also queued; CASE assigns to IO

### Sensitivity

Varying all numeric thresholds simultaneously by ±20%:

| Variation | Normal | CPU | IO | Network | Queue |
|-----------|--------|-----|-----|---------|-------|
| −20% | 91.52% | 2.80% | 0.22% | 4.74% | 0.72% |
| Baseline | 92.65% | 2.23% | 0.25% | 4.18% | 0.69% |
| +20% | 93.49% | 1.78% | 0.28% | 3.78% | 0.67% |

~1 percentage point swing across the board. Thresholds are not brittle.

---

## Implementation

| Location | Role |
|----------|------|
| `consumer_aggregate.py` → `classify_query()` | Streaming classification (per-message) |

---

## Full Analysis

For the complete numerical analysis (bucket breakdowns, transition-point identification, and raw statistics) that produced these thresholds, see **[QUERY_CLASSIFICATION_ANALYSIS.md](QUERY_CLASSIFICATION_ANALYSIS.md)**.
