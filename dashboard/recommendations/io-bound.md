**IO-bound**

These queries scan large volumes of data (≥ 5,000 MB) with a low execution-to-scan ratio (< 10 ms/MB) — they finish quickly per megabyte but are bottlenecked by the sheer amount of data Redshift must read from disk or cache.

**What's happening in your cluster:**
- Queries are performing full or near-full table scans across very large tables
- Disk I/O throughput is the limiting factor, not CPU processing power
- Spillage to disk may further degrade performance if intermediate results exceed available memory

**Actionable steps:**
- **Define or update SORTKEYs** — A SORTKEY on your most common filter columns (e.g., date, instance_id) allows Redshift to skip entire disk blocks. This is the single highest-impact optimisation for IO-bound queries
- **Avoid SELECT *** — Redshift is columnar; every extra column adds disk reads. Select only the columns you need to reduce scanned megabytes by 50–90%
- **Apply column-level compression** — Run `ANALYZE COMPRESSION` on your largest tables and re-encode columns. Proper encoding (AZ64 for timestamps, LZO for strings) reduces physical bytes on disk
- **Partition by time** — If queries naturally filter by date range, time-partition your tables. Combined with a date SORTKEY, this dramatically reduces scan scope
- **Add more memory** — If `mbytes_spilled` is also elevated, intermediate results are spilling to disk. Upgrading to nodes with more RAM (e.g., ra3.4xlarge → ra3.16xlarge) keeps more data in-memory