# rustify-temporal

A Temporal-compatible workflow server written in Rust. The official Temporal SDKs connect to it
exactly as they connect to the Temporal server: same gRPC API (`temporal.api.workflowservice.v1`),
same port (7233), no SDK changes. Everything is stored in one SQLite file.

Tested end to end with the official **Python SDK** (`temporalio` 1.36.1, 24 scenarios) and the
official **Go SDK** (`go.temporal.io/sdk` 1.51.0).

## Run it

```bash
cargo build --release
./target/release/rustify-temporal                 # listens on 127.0.0.1:7233, data in ./temporal.db
./target/release/rustify-temporal --ip 0.0.0.0 --port 7233 --db /var/lib/temporal.db --namespace orders
```

Options: `--ip`, `--port`, `--db` (`:memory:` keeps nothing), `--namespace` (repeatable; `default`
always exists). Logging follows `RUST_LOG` (default `info`).

Then point any SDK at it, for example in Python:

```python
client = await Client.connect("127.0.0.1:7233", namespace="default")
```

## What works

| Area | Supported |
|---|---|
| Namespaces | register, describe, list (paging, include-deleted), update (state, retention, bad binaries, replication checks), deprecate; namespace states enforced like the Go server (38-step side-by-side check); `default` created automatically |
| Workflows | start (id reuse and conflict policies, start delay, request-id dedupe), signal, signal-with-start, query, cancel, terminate, describe, history (long-poll, close-event filter), list/count |
| Workflow tasks | same behaviour as the Go server, checked in 67 side-by-side steps: transient retries with growing timeouts, sticky queues and their timeouts, refused completions (bad commands, unseen signals, bad builds), eager activities, payload/pending limits, continue-as-new suggestions, `return_new_workflow_task` |
| Commands | activities, timers, complete/fail/cancel, continue-as-new, markers (local activities, patches, side effects), search-attribute upsert, memo update, child workflows, signal external, cancel external |
| Activities | dispatch, heartbeats (with details and cancel requests), server-side retries with backoff, `next_retry_delay`, non-retryable types, schedule-to-start / start-to-close / schedule-to-close / heartbeat timeouts, complete/fail/cancel by token or by id |
| Workflow retries | retry policy on workflows, execution and run timeouts |
| Children | start, started/completed/failed/canceled/timed-out/terminated events, parent close policy (terminate, request cancel, abandon) |
| Queries | ride along on the next workflow task, or go to a worker as a query-only task; reject conditions |
| Updates | `UpdateWorkflowExecution` / `PollWorkflowExecutionUpdate`, same behaviour as the Go server (101 side-by-side steps): validators and rejections that leave no history, speculative workflow tasks, wait for accepted or completed, update-id dedupe, in-flight and total limits, aborts when the workflow closes or continues as new |
| Durability | every change is written to SQLite in one transaction; open runs, timers, queued tasks and activity retries are restored after a restart |
| Visibility | `ListWorkflowExecutions` / `CountWorkflowExecutions` with `WorkflowId`, `RunId`, `WorkflowType`, `TaskQueue`, `ExecutionStatus` filters joined with `AND` (`=` and `!=`) |
| Other | gRPC health service, gzip compression, `GetSystemInfo`, `GetClusterInfo`, `DescribeTaskQueue` (pollers) |

## Not yet

The full operation-by-operation comparison with the Go server is in [`PARITY.md`](PARITY.md)
(regenerate with `python3 scripts/parity.py > PARITY.md`).


These answer with a clear error (`unimplemented` or `invalid argument`) instead of misbehaving:

- Cron schedules and the Schedules API
- Nexus operations, update-with-start (`ExecuteMultiOperation`) and update completion callbacks
- Worker versioning / deployments, eager workflow start
- Reset, batch operations, archival, retention clean-up (closed runs are kept)
- Advanced visibility: custom search attributes in list filters, time ranges, `OR`, `ORDER BY`
- The Operator service (custom search attribute registration)
- Multi-node clustering, TLS and authorization
- The Temporal Web UI (it needs more of the API than is implemented)

## Performance vs the Go Temporal server

`bench/run.sh` runs the same workloads with the official Go SDK against rustify-temporal and against
the Go server built from this machine's Temporal source, both on SQLite, and writes
[`bench/RESULTS.md`](bench/RESULTS.md). Two runs on an Apple Silicon Mac (1000 workflows, 100 at once):

| | rustify vs Go server |
|---|---|
| Workflows per second (1 activity / 10 activities / signal + query) | 7-10x / 4x / 10x more |
| Time to get a workflow's result (p50) | 4-10x faster |
| Server CPU per run | about 10x less |
| Memory idle / under load | 24x / 3x less |
| Startup, database size, program size | 4-6x / 5x / 11x smaller |

These compare only what both servers do: the Go server also carries sharding, replication and many
features rustify-temporal doesn't have yet, which costs it some of this. See the caveats in
`bench/RESULTS.md`.

## Staying on the Go server's contract

`proto/` is a copy of the Go server's own contract, and all request, response and message types
are generated from it at build time, so their shapes can't drift from the Go server:

- the public API at exactly the version the Go server pins (`go.temporal.io/api` in its `go.mod`,
  resolved to the `temporalio/api` commit behind it), and
- the Go server's internal protos (`proto/internal`, from `temporalio/temporal/proto`), including the
  AdminService, which is served with its exact contract.

`proto/VERSION` records the versions. `scripts/sync-protos.sh` updates the copy from the Go server;
`scripts/sync-protos.sh --check` fails if it has drifted. The contract fixes the *shapes*; what
each operation *does* (validation, errors, ordering) is checked against the real Go server with
`scripts/namespace-parity.sh` and the SDK tests.

## How it works

```
SDK workers / clients ──gRPC──► service.rs (WorkflowService)
                                   │
                                   ▼
                              engine/ (one lock, all open runs in memory)
                               ├─ workflow.rs   start, signal, cancel, terminate, retries, children
                               ├─ wft.rs        workflow tasks and command processing
                               ├─ activity.rs   activity tasks, heartbeats, retries, timeouts
                               ├─ query.rs      queries
                               ├─ timers.rs     timer loop (user timers, timeouts, backoff)
                               ├─ events.rs     history events, buffering during a running task
                               └─ visibility.rs history reads, describe, list/count
                                   │
                                   ▼
                              store.rs (SQLite: namespaces, run state, events, current runs)
```

Each run's mutable state is itself a protobuf message (`state.rs`) saved next to its history
events. Task queues are in memory and long-polled; they are rebuilt from the saved state on start.
The protos under `proto/` are the Go server's contract (MIT, from temporalio/api and temporalio/temporal).

## Tests

GitHub Actions (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request:
formatting (rustfmt, ruff, gofmt), clippy with warnings as errors, release build, unit tests, the
Python and Go SDK end-to-end tests, the contract check against the Go server, and the namespace,
workflow-task and update side-by-side checks. Locally:

```bash
cargo test                                         # unit tests
./scripts/e2e.sh                                   # Python SDK + Go SDK end-to-end tests
./scripts/namespace-parity.sh                      # same namespace steps on the Go server and here, compared
./scripts/namespace-parity.sh e2e/wft_parity.py   # same workflow-task steps on both servers, compared
./scripts/namespace-parity.sh e2e/update_parity.py  # same workflow-update steps on both servers, compared
./scripts/sync-protos.sh --check                    # proto/ still matches the Go server's contract
```

`scripts/e2e.sh` builds the server, runs the Python suite (it starts its own server, including a
restart to check durability), then the Go checks. It needs `uv` and `go`.
