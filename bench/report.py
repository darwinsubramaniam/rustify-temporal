"""Turn the benchmark output folder into a Markdown comparison (stdout)."""

import json
import platform
import subprocess
import sys
from pathlib import Path

out, n, c = Path(sys.argv[1]), sys.argv[2], sys.argv[3]


def load(name):
    d = out / name
    return json.loads((d / "meta.json").read_text()), json.loads((d / "results.json").read_text())


def cpu_seconds(t: str) -> float:
    # ps time: [[dd-]hh:]mm:ss.cc
    parts = t.replace("-", ":").split(":")
    s = 0.0
    for p in parts:
        s = s * 60 + float(p)
    return s


rm, rr = load("rustify")
gm, gr = load("go")


def ratio(rust, go, higher_is_better):
    if not rust or not go:
        return "n/a"
    x = rust / go if higher_is_better else go / rust
    return f"**{x:.1f}x {'better' if x >= 1 else 'worse'}**"


rows = []
for a, b in zip(rr, gr):
    rows.append(
        f"| {a['scenario']} | {a['workflows_per_second']:.0f} / s | {b['workflows_per_second']:.0f} / s | "
        f"{ratio(a['workflows_per_second'], b['workflows_per_second'], True)} | "
        f"{a['p50_ms']:.1f} / {a['p95_ms']:.1f} / {a['p99_ms']:.1f} | {b['p50_ms']:.1f} / {b['p95_ms']:.1f} / {b['p99_ms']:.1f} | "
        f"{ratio(a['p50_ms'], b['p50_ms'], False)} | {a['errors']} / {b['errors']} |"
    )

rcpu = cpu_seconds(rm["cpu_after"]) - cpu_seconds(rm["cpu_before"])
gcpu = cpu_seconds(gm["cpu_after"]) - cpu_seconds(gm["cpu_before"])
mb = lambda kb: kb / 1024
res = [
    (
        "Startup until accepting connections (median of 3)",
        f"{rm['startup_ms']} ms",
        f"{gm['startup_ms']} ms",
        ratio(rm["startup_ms"], gm["startup_ms"], False),
    ),
    (
        "Memory when idle",
        f"{mb(rm['idle_rss_kb']):.0f} MB",
        f"{mb(gm['idle_rss_kb']):.0f} MB",
        ratio(rm["idle_rss_kb"], gm["idle_rss_kb"], False),
    ),
    (
        "Peak memory under load",
        f"{mb(rm['peak_rss_kb']):.0f} MB",
        f"{mb(gm['peak_rss_kb']):.0f} MB",
        ratio(rm["peak_rss_kb"], gm["peak_rss_kb"], False),
    ),
    ("Server CPU time for the whole run", f"{rcpu:.1f} s", f"{gcpu:.1f} s", ratio(rcpu, gcpu, False)),
    (
        "Database size afterwards",
        f"{rm['db_bytes'] / 1e6:.1f} MB",
        f"{gm['db_bytes'] / 1e6:.1f} MB",
        ratio(rm["db_bytes"], gm["db_bytes"], False),
    ),
    (
        "Program size",
        f"{rm['binary_bytes'] / 1e6:.0f} MB",
        f"{gm['binary_bytes'] / 1e6:.0f} MB",
        ratio(rm["binary_bytes"], gm["binary_bytes"], False),
    ),
]
go_version = subprocess.run(["go", "version"], capture_output=True, text=True).stdout.strip()
rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()

print(f"""# rustify-temporal vs the Go Temporal server

Same machine, same workloads, same client: the official Temporal **Go SDK** (one client and 4
workers, each with 16 workflow and 16 activity pollers, in the benchmark program; the Go SDK caps
each worker at 1000 polls/s, so one worker would cap both servers near 500 workflows/s) runs each scenario against each server in turn. Both servers keep
their data in **SQLite** (WAL, synchronous=NORMAL) in a fresh folder. The Go server is the
single-process build of this repository with its `development-sqlite-file.yaml` config (1 history
shard). Only features both servers support are measured. {n} workflows per scenario, {c} running at
once (fan-out: a fifth of each), after a 50-workflow warm-up.

Machine: {platform.machine()} {platform.system()} {platform.release()} · {go_version} · {rustc or "rustc"}

## Throughput and latency

| Scenario | rustify | Go server | Throughput | Latency p50 / p95 / p99 ms (rustify) | Latency p50 / p95 / p99 ms (Go) | Latency (p50) | Errors (rustify / Go) |
|---|---|---|---|---|---|---|---|
{chr(10).join(rows)}

## Resources

| Measure | rustify | Go server | Difference |
|---|---|---|---|
{chr(10).join(f"| {a} | {b} | {c_} | {d} |" for a, b, c_, d in res)}

"Better" means faster, quicker or smaller for rustify. Latency is the time from starting a workflow
to having its result.

## Caveats

- One run on one machine; numbers move a few percent between runs. Run `bench/run.sh` again to check.
- The Go server is built for clusters (sharding, replication, many more features); rustify-temporal
  is a single process that implements 33 of 123 workflow-service operations. Part of the gap is that
  extra work the Go server does, not only the language.
- Neither server was tuned. The Go server keeps its default dynamic config.
""")
