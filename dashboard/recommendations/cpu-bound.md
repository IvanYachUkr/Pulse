**CPU-bound**

These queries scan moderate data volumes (50–5,000 MB) but spend disproportionate compute time on joins and aggregations — the execution-to-scan ratio exceeds 1,000 ms/MB, meaning the bottleneck is processing, not reading.

**What's happening in your cluster:**
- Complex joins and aggregations are consuming CPU cycles far beyond what the data volume warrants
- The model flagged these because execution time is orders of magnitude higher than expected for the amount of data scanned

**Actionable steps:**
- **Audit join distribution keys** — If the join columns don't match the table distribution keys, Redshift must redistribute (shuffle) rows across nodes for every join. Aligning DISTKEY on your most-joined columns eliminates this overhead entirely
- **Pre-aggregate staging tables** — If the same multi-table aggregation runs repeatedly, materialise the intermediate result into a staging table. This shifts computation from query-time to ETL-time
- **Reduce join fan-out** — Check for unintentional cross-joins or many-to-many joins that explode row counts. Add explicit WHERE clauses to limit join scope
- **Consider node upgrade** — If joins and aggregations are inherent to the workload, a node type with more CPU cores (e.g., ra3.4xlarge → ra3.16xlarge) provides linear compute scaling