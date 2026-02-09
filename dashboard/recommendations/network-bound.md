**Network-bound**

These are COPY or UNLOAD operations that take over 1 second to complete — the bottleneck is data transfer between Redshift and external storage (typically S3), not query processing.

**What's happening in your cluster:**
- Bulk data loads (COPY) or exports (UNLOAD) are transferring large volumes over the network
- Long-running transfers compete for network bandwidth with other concurrent operations
- Poorly structured source files (too many small files, or a single massive file) cause uneven node utilisation

**Actionable steps:**
- **Split files to match node count** — For COPY, split input files into multiples of your slice count (e.g., 16 slices = 16 or 32 files). Each slice reads one file in parallel; imbalanced splits leave nodes idle
- **Compress transfer data** — Use GZIP or ZSTD compression on S3 files to reduce network bytes by 60–80%. The decompression CPU cost is negligible compared to transfer savings
- **Target file sizes of 1–4 GB** — Many small files (< 100 MB) add per-file network overhead. A single 50 GB file prevents parallelism. The 1–4 GB range balances both
- **Use manifest files** — Explicit manifests let you control exactly which files are loaded, avoiding accidental re-processing and enabling incremental loads
- **Schedule bulk transfers off-peak** — COPY/UNLOAD operations saturate network I/O. Run them during low-query windows to prevent impacting interactive workloads
- **Consider Redshift Spectrum** — For read-only external data, query it in-place on S3 via Spectrum to avoid the COPY step entirely