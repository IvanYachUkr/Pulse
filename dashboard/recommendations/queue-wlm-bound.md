**Queue/WLM-bound**

These queries have significant queue wait time (> 1 second) relative to their execution profile — they sat in a WLM queue waiting for a slot instead of running. The query itself is not cached, has moderate compute characteristics, yet is delayed by concurrency contention.

**What's happening in your cluster:**
- More queries are submitted concurrently than the WLM queues can serve
- Heavy ETL jobs and short interactive queries compete for the same queue slots
- Queue wait time inflates total wall-clock time even when the query itself runs fast

**Actionable steps:**
- **Enable Short Query Acceleration (SQA)** — SQA automatically routes short queries (< ~10s execution) to a dedicated fast lane, preventing them from queuing behind hour-long ETL jobs
- **Separate WLM queues by workload** — Create distinct queues: one for interactive/BI queries (high concurrency, low memory) and one for ETL/batch loads (low concurrency, high memory). Assign user groups accordingly
- **Enable concurrency scaling** — Redshift can automatically spin up transient clusters during peak demand. This adds capacity on-demand without permanently upsizing
- **Stagger ETL schedules** — If queue contention spikes during specific hours, shift batch jobs to off-peak windows. The REDSET data often shows predictable daily queue patterns
- **Review max concurrency settings** — If your WLM queue has concurrency set too low, increase it (with lower per-query memory). If set too high, queries compete for memory and spill. Find the balance for your workload