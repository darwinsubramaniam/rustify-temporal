# rustify-temporal vs the Go Temporal server

Same machine, same workloads, same client: the official Temporal **Go SDK** (one client and 4
workers, each with 16 workflow and 16 activity pollers, in the benchmark program; the Go SDK caps
each worker at 1000 polls/s, so one worker would cap both servers near 500 workflows/s) runs each scenario against each server in turn. Both servers keep
their data in **SQLite** (WAL, synchronous=NORMAL) in a fresh folder. The Go server is the
single-process build of this repository with its `development-sqlite-file.yaml` config (1 history
shard). Only features both servers support are measured. 1000 workflows per scenario, 100 running at
once (fan-out: a fifth of each), after a 50-workflow warm-up.

Machine: arm64 Darwin 25.5.0 · go version go1.27.1 darwin/arm64 · rustc 1.100.0 (0ef1bc386 2026-11-09)

## Throughput and latency

| Scenario | rustify | Go server | Throughput | Latency p50 / p95 / p99 ms (rustify) | Latency p50 / p95 / p99 ms (Go) | Latency (p50) | Errors (rustify / Go) |
|---|---|---|---|---|---|---|---|
| workflow with 1 activity | 2000 / s | 205 / s | **9.7x better** | 48.6 / 59.1 / 63.2 | 481.9 / 590.9 / 632.8 | **9.9x better** | 0 / 0 |
| workflow with 10 parallel activities | 410 / s | 99 / s | **4.2x better** | 52.1 / 58.1 / 61.0 | 202.3 / 311.3 / 331.5 | **3.9x better** | 0 / 0 |
| signal + query + result | 1966 / s | 192 / s | **10.3x better** | 46.1 / 64.7 / 68.0 | 499.9 / 640.8 / 684.2 | **10.8x better** | 0 / 0 |

## Resources

| Measure | rustify | Go server | Difference |
|---|---|---|---|
| Startup until accepting connections (median of 3) | 15 ms | 57 ms | **3.8x better** |
| Memory when idle | 6 MB | 136 MB | **24.1x better** |
| Peak memory under load | 134 MB | 373 MB | **2.8x better** |
| Server CPU time for the whole run | 2.2 s | 21.9 s | **10.0x better** |
| Database size afterwards | 8.7 MB | 43.2 MB | **5.0x better** |
| Program size | 15 MB | 163 MB | **11.0x better** |

"Better" means faster, quicker or smaller for rustify. Latency is the time from starting a workflow
to having its result.

## Caveats

- One run on one machine; numbers move a few percent between runs. Run `bench/run.sh` again to check.
- The Go server is built for clusters (sharding, replication, many more features); rustify-temporal
  is a single process that implements 33 of 123 workflow-service operations. Part of the gap is that
  extra work the Go server does, not only the language.
- Neither server was tuned. The Go server keeps its default dynamic config.

