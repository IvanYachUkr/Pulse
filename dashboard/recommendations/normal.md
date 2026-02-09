**Healthy workload**

The selected instances show no dominant bottleneck pattern — queries are executing within normal parameters for their data volume, compute complexity, and queue wait time.

**What this means:**
- Execution-to-scan ratios are within expected ranges
- No significant queue contention detected
- COPY/UNLOAD operations (if any) complete within acceptable timeframes

**Keep monitoring for:**
- **Trend shifts** — Use the time-series chart above to watch for emerging patterns. A gradual increase in any classification category may indicate a developing issue before it becomes critical
- **Spillage growth** — Even without a dominant bottleneck, rising `mbytes_spilled` values signal that intermediate results are growing beyond available memory
- **Anomaly spikes** — The ML anomaly detection runs independently and may flag individual outlier queries even when the overall classification is healthy