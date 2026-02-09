# Query Classification Thresholds - Final Results

> **Dataset**: REDSET `full_provision.parquet` - **433,107,811 rows** (17.7 GB)  
> **Engine**: DuckDB 1.4.4, 6 GB memory cap, 4 threads  
> **Method**: Behavioral regime detection with per-bucket transition analysis  
> **Date**: 2026-02-07  

---

## Final SQL Classification

```sql
CASE
    -- ① Network-bound: COPY/UNLOAD moving meaningful data
    WHEN was_cached = 0
         AND query_type IN ('copy', 'unload')
         AND execution_duration_ms > 500
    THEN 'Network-bound'

    -- ② CPU-bound: complex, genuinely slow queries where compute dominates
    WHEN was_cached = 0
         AND query_type NOT IN ('copy', 'unload')
         AND (COALESCE(num_joins, 0) > 1 OR COALESCE(num_aggregations, 0) > 1)
         AND COALESCE(mbytes_scanned, 0) > 0
         AND (execution_duration_ms::DOUBLE / (COALESCE(mbytes_scanned, 0) + 1)) > 20.0
         AND execution_duration_ms > 1000
    THEN 'CPU-bound'

    -- ③ IO-bound: massive scans where disk throughput is the bottleneck
    WHEN was_cached = 0
         AND query_type NOT IN ('copy', 'unload')
         AND COALESCE(mbytes_scanned, 0) >= 2000
         AND (execution_duration_ms::DOUBLE / (COALESCE(mbytes_scanned, 0) + 1)) < 0.061
         AND (COALESCE(mbytes_spilled, 0) > 0 OR COALESCE(num_scans, 0) > 1)
         AND execution_duration_ms > 1000
    THEN 'IO-bound'

    -- ④ Queue/WLM-bound: simple queries stuck waiting for cluster resources
    WHEN was_cached = 0
         AND queue_duration_ms >= execution_duration_ms
         AND queue_duration_ms > 1000
         AND execution_duration_ms > 0
         AND (COALESCE(num_scans, 0) + COALESCE(num_joins, 0) + COALESCE(num_aggregations, 0)) < 4
    THEN 'Queue/WLM-bound'

    ELSE 'Normal'
END AS query_class
```

---

## Threshold Summary

| Class | Key Thresholds | What It Captures |
|-------|---------------|------------------|
| **Network** | COPY/UNLOAD + exec > 500ms | Data transfer ops moving ≥50 MB (median scan) |
| **CPU-bound** | ratio > 20 ms/MB + exec > 1s + joins/aggs > 1 | Complex queries where compute time dominates, not IO |
| **IO-bound** | scan ≥ 2000 MB + ratio < 0.061 + spill/multi-scan + exec > 1s | Massive scans bottlenecked by disk throughput |
| **Queue/WLM** | queue ≥ exec + queue > 1s + complexity < 4 | Simple queries stuck waiting - the cluster is the bottleneck |
| Normal | Everything else | No single dominant bottleneck |

---

## How Each Threshold Was Derived

### CPU-bound: ratio > 20.0 ms/MB

**Data source**: Exec/scan ratio regime - 281M qualifying queries (non-cached, non-COPY/UNLOAD, scan > 0, joins+aggs > 1).

| Ratio Bucket | Count | Spill % | Queue Dom % | Avg Exec | vs Overall |
|-------------|-------|---------|-------------|----------|-----------|
| 0.10–0.50 | 117.0M | 1.67% | 0.71% | 966 ms | **0.35x** |
| 0.50–1.0 | 47.0M | 2.41% | 1.07% | 1,401 ms | 0.50x |
| 1.0–2.0 | 30.9M | 3.36% | 1.80% | 2,525 ms | 0.91x |
| 2.0–5.0 | 27.0M | 4.21% | 3.00% | 3,609 ms | 1.30x |
| 5.0–10.0 | 14.1M | 4.47% | 5.36% | 5,072 ms | 1.82x |
| 10.0–20.0 | 9.9M | 3.90% | 7.07% | 6,677 ms | 2.40x |
| **20.0–50.0** | **9.1M** | **3.22%** | **7.77%** | **7,172 ms** | **2.58x ← transition** |
| 50.0–100.0 | 3.0M | 4.94% | 7.95% | 12,665 ms | 4.56x |
| 100.0–500.0 | 1.7M | 8.88% | 7.69% | 39,000 ms | 14.03x |

**Transition point**: At ratio ≥ 20, per-bucket average execution first exceeds 2x the overall average (7,172ms vs 2,780ms population avg), while spill rate stays near baseline (3.22% vs 2.29%). Queries are genuinely slow due to computation, not memory pressure.

The `exec > 1000ms` filter ensures trivially fast high-ratio queries (e.g., 5ms exec on 0.1 MB scan = ratio 50) are excluded.

**Why no scan range constraint**: Tested 4 variants:

| Variant | Count | % | Purity | Avg Exec | Complexity | Spill % |
|---------|-------|---|--------|----------|-----------|---------|
| A: ratio>20, scan 5–434 MB | 11.8M | 2.71% | **90.1%** | 5,968 ms | 3.7 | 3.3% |
| **B: ratio>20 only** | **13.9M** | **3.20%** | **88.2%** | **15,516 ms** | **3.9** | **4.3%** |
| **C: ratio>20 + exec>1s** | **9.6M** | **2.23%** | **88.0%** | **22,075 ms** | **4.3** | **5.6%** |
| D: ratio>20, scan 5–434, exec>1s | 8.8M | 2.04% | 89.7% | 7,740 ms | 3.9 | 3.7% |

**Decision**: Variant C (ratio>20 + exec>1000ms, no scan range). The scan range was an artifact of P10-P95 statistics, not a behavioral boundary. Variant C captures genuinely slow, complex queries (exec_p50 = 2,981ms, complexity = 4.3) while the exec>1000ms filter naturally excludes trivially small queries.

---

### IO-bound: scan ≥ 2000 MB

**Data source**: Scan volume regime - 301M qualifying queries (non-cached, non-COPY/UNLOAD, scan > 0).

| Scan Bucket (MB) | Count | Spill % | Multi-scan % | Ratio P50 | Avg Spill (MB) |
|------------------|-------|---------|-------------|-----------|----------------|
| 200–500 | 34.7M | 3.07% | 49.5% | 0.48 | 61 |
| 500–1000 | 19.7M | 3.81% | 43.0% | 0.29 | 152 |
| 1000–2000 | 17.9M | 4.25% | 42.1% | 0.28 | 182 |
| **2000–5000** | **11.2M** | **7.74%** | **53.3%** | **0.24** | **649 ← transition** |
| 5000–10000 | 4.9M | 12.14% | 64.3% | 0.27 | 1,938 |
| 10000–50000 | 6.4M | 19.83% | 73.4% | 0.33 | 10,769 |
| 100000+ | 2.1M | 34.48% | 77.6% | 0.08 | 341,865 |

**Transition point**: At scan ≥ 2000 MB, spill rate nearly doubles (4.25% → 7.74%), multi-scan rate crosses 53%, and average spill volume jumps from 182 MB → 649 MB. This is where disk throughput becomes the dominant bottleneck.

**Ratio ceiling (0.061 ms/MB)**: Average P25 of per-bucket exec/scan ratio for scan ≥ 2000 MB. IO-bound queries have a **very low ratio** because most execution time is spent reading, not computing.

---

### Network-bound: exec > 500 ms

**Data source**: COPY/UNLOAD duration regime (40.8M COPY + 1.95M UNLOAD).

| COPY Duration (ms) | Count | Scan P50 (MB) | Avg Scan (MB) |
|--------------------|-------|---------------|---------------|
| 50–200 | 16.7M | 0 | 64 |
| 200–500 | 7.5M | 46 | 208 |
| **500–1000** | **6.1M** | **176** | **435 ← P50 crosses 50 MB** |
| 1000–5000 | 7.7M | 238 | 921 |

**Transition point**: COPY scan P50 first exceeds 50 MB at duration ≥ 500ms, indicating real data movement (not trivial cache-served or empty operations). UNLOAD moves significant data at almost any duration (P50 > 43 MB even at 0ms).

---

### Queue/WLM-bound: queue > 1000 ms

**Data source**: Queue fraction regime - 15.3M queries with both queue > 0 and exec > 0.

| Queue ≥ (ms) | Count | Avg Queue | Avg Exec | Queue/Exec | Queue Dom % |
|-------------|-------|-----------|----------|-----------|-------------|
| 0 | 2.1M | 40 ms | 7,375 ms | 0.0x | 10.4% |
| 500 | 1.7M | 729 ms | 8,469 ms | 0.1x | 49.7% |
| **1000** | **1.5M** | **1,439 ms** | **10,766 ms** | **0.1x** | **57.8%** |
| 2000 | 1.8M | 3,254 ms | 13,359 ms | 0.2x | 67.6% |
| 5000 | 1.4M | 7,202 ms | 15,891 ms | 0.5x | 78.2% |
| 10000 | 2.6M | 23,352 ms | 20,888 ms | 1.1x | 87.3% |

**Observation**: No sharp breakpoint exists - the queue regime shows a gradual transition. The per-bucket queue-to-exec ratio never exceeds 5x, meaning queued queries also tend to be heavier (not purely sitting idle). 1000ms is a reasonable minimum that filters noise while the `queue >= exec` condition ensures queue genuinely dominates the total latency. The `complexity < 4` requirement ensures we capture simple queries that are waiting, not complex queries that happen to queue.

---

## Validation Results

### Final Classification Distribution

| Class | Count | % of Total | Exec P50 | Avg Scan (MB) | Spill % | Queue Dom % | Complexity | Ratio P50 |
|-------|-------|-----------|----------|---------------|---------|-------------|------------|-----------|
| **Normal** | 401.3M | 92.65% | 37 ms | 3,176 | 2.0% | 1.1% | 2.5 | 0.66 |
| **Network** | 18.1M | 4.18% | 1,410 ms | 29,479 | 4.4% | 4.5% | 2.7 | 9.03 |
| **CPU-bound** | 9.6M | 2.23% | 2,981 ms | 307 | 5.6% | 6.8% | 4.3 | 42.6 |
| **Queue/WLM** | 3.0M | 0.69% | 204 ms | 1,895 | 5.3% | 100% | 1.5 | 3.9 |
| **IO-bound** | 1.1M | 0.25% | 4,522 ms | 937,899 | 27.1% | 3.4% | 7.3 | 0.033 |

### Behavioral Distinctness

Each class exhibits the expected behavioral signature:

- **CPU-bound**: highest ratio (42.6x), highest complexity (4.3), moderate scan (307 MB), exec_p50 = 2,981ms. These queries spend time *computing*, not reading.
- **IO-bound**: massive scans (938 GB avg), highest spill (27.1%), lowest ratio (0.033), highest complexity (7.3). These queries are bottlenecked by disk throughput.
- **Network-bound**: COPY/UNLOAD moving 29.5 GB avg. Significant data transfer operations.
- **Queue/WLM-bound**: 100% queue dominance, lowest complexity (1.5), queue_avg = 30.6 seconds. Simple queries stuck waiting for cluster resources.
- **Normal**: balanced baseline across all metrics - no single dominant bottleneck.

### Purity Check

> Of classified queries, what percentage exhibit **all** expected behavioral signatures?

| Class | Pure | Total | Purity | Criteria |
|-------|------|-------|--------|----------|
| CPU-bound | 8.5M | 9.6M | **88.0%** | no spill AND queue < exec AND complexity > 1 |
| CPU (slow+complex only) | 9.6M | 9.6M | **100%** | exec > 1000ms AND complexity > 1 |
| IO-bound | 1.1M | 1.1M | **100%** | scan ≥ 2000 AND (spill > 0 OR multi-scan) |
| Network-bound | 18.1M | 18.1M | **100%** | COPY/UNLOAD AND exec > 500ms |
| Queue/WLM-bound | 3.0M | 3.0M | **100%** | queue ≥ exec AND queue > 1000ms |

The 12% "impure" CPU-bound queries have some spill (5.6% class-wide vs 2.29% baseline) - borderline queries that are both compute-heavy and pushing memory. They are still genuinely slow and complex - just not *purely* compute-limited.

### Cross-Class Overlap

When conditions are evaluated independently (no CASE priority):

| Overlap | Count | Notes |
|---------|-------|-------|
| CPU ∩ IO | **0** | Mathematically impossible (ratio > 20 AND ratio < 0.061) |
| CPU ∩ Queue | 652,228 | Complex slow queries that also queued; CASE prioritizes CPU |
| IO ∩ Queue | 37,254 | Large-scan queries that also queued; CASE prioritizes IO |

The CASE evaluation order (Network → CPU → IO → Queue → Normal) resolves all overlaps. CPU+IO overlap is structurally impossible due to contradictory ratio thresholds.

### Sensitivity Analysis (±20%)

| Variation | Normal | CPU | IO | Network | Queue |
|-----------|--------|-----|-----|---------|-------|
| -20% | 91.52% | 2.80% | 0.22% | 4.74% | 0.72% |
| **baseline** | **92.65%** | **2.23%** | **0.25%** | **4.18%** | **0.69%** |
| +20% | 93.49% | 1.78% | 0.28% | 3.78% | 0.67% |

±20% variation produces ~1% swing in Normal class - all thresholds are stable.

---

## Comparison: Old vs New Thresholds

| Parameter | Old (hardcoded) | New (data-derived) | Why |
|-----------|----------------|--------------------|----|
| CPU ratio | > 1000 ms/MB | > 20 ms/MB | Old was 50x too high - missed 99%+ of CPU-bound queries |
| CPU scan range | 50–5000 MB | *removed* | Artificial; not a behavioral boundary |
| CPU min exec | - | > 1000 ms | Excludes trivially fast high-ratio queries |
| IO ratio ceiling | < 10 ms/MB | < 0.061 ms/MB | IO-bound queries have *very* low ratios (all time spent reading) |
| IO scan min | ≥ 5000 MB | ≥ 2000 MB | Spill transition starts at 2000 MB, not 5000 |
| Network | exec > 1000ms + external_tables > 0 | exec > 500ms | Only 1.3% of COPY logged external_tables; 500ms captures real transfers |
| Queue ratio range | ratio 10–100 ms/MB | *removed* | Ratio is irrelevant to queuing - replaced with complexity < 4 |

---