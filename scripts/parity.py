"""Write PARITY.md: every operation of the Go Temporal server's public APIs, and whether
rustify-temporal has it, plus the server features that aren't API operations.

Usage: python3 scripts/parity.py [path/to/temporal-go-source] > PARITY.md

Operations come from the vendored protos (and the Go repo's internal admin proto when given);
"implemented" means src/service.rs handles the method. The notes below say where an implemented
operation is only partly there.
"""

import re
import sys
from collections import OrderedDict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GO_SRC = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT.parent / "temporal"


def rpcs(path: Path) -> list[str]:
    return re.findall(r"\brpc\s+(\w+)\s*\(", path.read_text())


def snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


implemented = set(re.findall(r"async fn (\w+)\(", (ROOT / "src/service.rs").read_text()))

# Where an implemented operation is only partly there.
PARTIAL = {
    "StartWorkflowExecution": "No cron schedules, eager workflow start, completion callbacks, versioning override or links.",
    "SignalWithStartWorkflowExecution": "Same limits as StartWorkflowExecution.",
    "GetWorkflowExecutionHistory": "Whole history in one page (no page size), no raw history blobs, no archival.",
    "ListWorkflowExecutions": "Filters on WorkflowId, RunId, WorkflowType, TaskQueue, ExecutionStatus with = / != and AND only.",
    "CountWorkflowExecutions": "Same filters as List; no GROUP BY.",
    "DescribeTaskQueue": "Recent pollers only; no backlog stats, rate limits or versioning info.",
    "DescribeWorkflowExecution": "No callbacks, pending Nexus operations or versioning info.",
    "RespondWorkflowTaskCompleted": "Matches the Go server (67-step check), including eager activities, size limits and retries; workflow-update messages and Nexus commands belong to those missing areas.",
    "PollWorkflowTaskQueue": "Matches the Go server (67-step check); worker-versioning and poller-autoscaling fields belong to those missing areas.",
    "PollActivityTaskQueue": "No worker versioning, rate limiting or poller autoscaling hints.",
    "RecordActivityTaskHeartbeat": "Never reports activity paused or reset (those features are missing).",
    "RecordActivityTaskHeartbeatById": "Same as RecordActivityTaskHeartbeat.",
    "GetSystemInfo": "Reports the capabilities this server has (no schedules, Nexus, eager start, versioning).",
    "GetClusterInfo": "Fixed single-cluster answer.",
    "ShutdownWorker": "Accepted and ignored.",
    "RecordWorkerHeartbeat": "Accepted and ignored (worker list not kept).",
}

# Notes for operations that match the Go server.
NOTES = {
    "RegisterNamespace": "Same validation as the Go server (retention of at least 1 hour, cluster names). Global namespaces are refused and archival stays off, as on a Go server without them enabled.",
    "UpdateNamespace": "Description, owner, data, state changes, retention, bad binaries (max 10), aliases, replication checks; promotion refused as on a single-cluster Go server.",
    "ListNamespaces": "Paging and the include-deleted filter.",
    "DescribeNamespace": "By name or id.",
    "DeprecateNamespace": "New workflows are refused; running ones carry on.",
    "RespondWorkflowTaskFailed": "Retries are transient (not written to history) like the Go server.",
    "ResetStickyTaskQueue": "A waiting sticky task moves to the normal queue.",
}

GROUPS = [
    ("Namespaces", r"Namespace"),
    ("Workflow tasks", r"WorkflowTask|StickyTaskQueue"),
    (
        "Activity tasks (workflow activities)",
        r"^(PollActivityTaskQueue|RecordActivityTaskHeartbeat|RespondActivityTask)",
    ),
    ("Standalone activities and activity control", r"Activit"),
    ("Queries", r"Query"),
    ("Workflow updates", r"^(UpdateWorkflowExecution|PollWorkflowExecutionUpdate)$"),
    ("Schedules", r"Schedule"),
    ("Batch operations", r"Batch"),
    ("Nexus", r"Nexus"),
    ("Worker versioning and deployments", r"Deployment|BuildId|Versioning|Reachability|WorkerVersion"),
    ("Workers and task queues", r"Worker|TaskQueue|Poll"),
    ("Workflow rules", r"WorkflowRule"),
    ("Visibility (listing and searching runs)", r"List|Count|Scan|SearchAttributes"),
    ("Server and cluster info", r"SystemInfo|ClusterInfo"),
    ("Running workflows (start, signal, cancel, history...)", r"."),
]


def group_of(name: str) -> str:
    for title, pat in GROUPS:
        if re.search(pat, name):
            return title
    return GROUPS[-1][0]


def status(name: str) -> str:
    if snake(name) not in implemented:
        return "❌ No"
    return "🟡 Partial" if name in PARTIAL else "✅ Yes"


wf = rpcs(ROOT / "proto/temporal/api/workflowservice/v1/service.proto")
op = rpcs(ROOT / "proto/temporal/api/operatorservice/v1/service.proto")
admin_path = ROOT / "proto/internal/temporal/server/api/adminservice/v1/service.proto"
admin = rpcs(admin_path) if admin_path.exists() else []

grouped: "OrderedDict[str, list[str]]" = OrderedDict((t, []) for t, _ in GROUPS)
for r in wf:
    grouped[group_of(r)].append(r)

counts = {"✅ Yes": 0, "🟡 Partial": 0, "❌ No": 0}
for r in wf:
    counts[status(r)] += 1

out = []
p = out.append
p("# Parity: Go Temporal server vs rustify-temporal\n")
p("Generated by `scripts/parity.py` from the API definitions: every operation the Go Temporal server")
p("exposes, and whether rustify-temporal has it. ✅ Yes = works as SDKs expect; 🟡 Partial = works for")
p("the common case, with the limits noted; ❌ No = answers *unimplemented*.\n")
version = {
    l.split()[0]: l.split()[1] for l in (ROOT / "proto/VERSION").read_text().splitlines() if l and not l.startswith("#")
}
p(
    f"Contract: `go.temporal.io/api {version.get('go.temporal.io/api', '?')}` (temporalio/api `{version.get('temporalio/api', '?')[:12]}`), the"
)
p("exact version the Go server pins, plus the Go server's internal protos. `scripts/sync-protos.sh --check`")
p("fails if `proto/` drifts from it. Every operation below is served with that exact request and")
p("response shape, including the ones that answer *unimplemented*.\n")
p("## Summary\n")
p("| API | Operations | ✅ Yes | 🟡 Partial | ❌ No |")
p("|---|---|---|---|---|")
p(
    f"| WorkflowService (what SDKs and the CLI use) | {len(wf)} | {counts['✅ Yes']} | {counts['🟡 Partial']} | {counts['❌ No']} |"
)
p(f"| OperatorService (search attributes, clusters, Nexus endpoints) | {len(op)} | 0 | 0 | {len(op)} |")
if admin:
    p(f"| AdminService (internal: tdbg, replication, DLQ tools) | {len(admin)} | 0 | 0 | {len(admin)} |")
p("")
p("Behaviour is checked step by step against the Go server with `scripts/namespace-parity.sh`:")
p("namespaces (38 steps) and workflow tasks (`e2e/wft_parity.py`, 67 steps), all identical. Deprecated")
p("namespaces refuse new workflows, deleted ones refuse everything but describe and list.\n")
p("Everything a worker needs to run workflows (start, workflow and activity tasks, timers, signals,")
p("queries, children, continue-as-new, retries, cancel, terminate, history) is in the ✅ and 🟡 rows.\n")

p("## WorkflowService, by area\n")
for title, names in grouped.items():
    if not names:
        continue
    done = sum(1 for n in names if status(n) != "❌ No")
    p(f"### {title} ({done}/{len(names)})\n")
    p("| Operation | rustify | Notes |")
    p("|---|---|---|")
    for n in sorted(names, key=lambda x: (status(x) == "❌ No", x)):
        p(f"| `{n}` | {status(n)} | {PARTIAL.get(n, NOTES.get(n, '') if status(n) != '❌ No' else '')} |")
    p("")

p("## OperatorService\n")
p("| Operation | rustify |")
p("|---|---|")
for n in op:
    p(f"| `{n}` | ❌ No |")
p("")
if admin:
    p("## AdminService (internal)\n")
    p(
        "Used by `tdbg`, `temporal operator` admin commands and replication. Served with its exact contract; every operation answers *unimplemented* for now.\n"
    )
    p(", ".join(f"`{n}`" for n in admin))
    p("")

p("## Workflow behaviour (inside the operations above)\n")
p("| Feature | Go server | rustify | Notes |")
p("|---|---|---|---|")
for row in [
    (
        "Activities, retries with backoff, all four timeouts, heartbeats",
        "✅",
        "✅ Yes",
        "Server-side retries, `next_retry_delay`, non-retryable types.",
    ),
    ("Timers (start, cancel, fire)", "✅", "✅ Yes", ""),
    ("Signals, signal-with-start, signal external workflow", "✅", "✅ Yes", "Request-id de-duplication for signals."),
    ("Queries (with a running task, query-only tasks, reject conditions)", "✅", "✅ Yes", ""),
    ("Child workflows and parent close policies", "✅", "✅ Yes", "Terminate, request-cancel, abandon."),
    ("Cancel workflow, cancel external, cancel activity", "✅", "✅ Yes", ""),
    ("Continue-as-new and workflow retry policies", "✅", "✅ Yes", ""),
    ("Execution and run timeouts", "✅", "✅ Yes", ""),
    ("Local activities, side effects, patching (markers)", "✅", "✅ Yes", "Stored as markers like the Go server."),
    ("Sticky queues and partial history", "✅", "✅ Yes", "Falls back to the normal queue after the sticky timeout."),
    (
        "Workflow-task failure, timeout and retry",
        "✅",
        "✅ Yes",
        "Retries after the first failure are transient (written only if they complete); later attempts get longer timeouts.",
    ),
    ("Workflow id reuse and conflict policies", "✅", "✅ Yes", ""),
    (
        "Search attribute upsert and memo update",
        "✅",
        "🟡 Partial",
        "Stored and shown; not searchable in list filters.",
    ),
    ("Start delay", "✅", "✅ Yes", ""),
    ("Cron schedules", "✅", "❌ No", "Rejected with a clear error."),
    ("Workflow updates", "✅", "❌ No", ""),
    ("Nexus operations", "✅", "❌ No", ""),
    ("Reset, pause/unpause, delete workflow", "✅", "❌ No", ""),
    (
        "Sticky queues: schedule-to-start timeout, reset",
        "✅",
        "✅ Yes",
        "The timeout is recorded and the task moves to the normal queue.",
    ),
    (
        "Refused completions (bad commands, unseen signals, bad builds)",
        "✅",
        "✅ Yes",
        "Same causes and errors; a refused retry leaves the task running.",
    ),
    (
        "Payload and pending-work limits",
        "✅",
        "✅ Yes",
        "2 MB payloads (terminates), 2000 pending activities or children.",
    ),
    ("Continue-as-new suggestion", "✅", "✅ Yes", "At 4096 events or 4 MB of history."),
    ("Eager activities", "✅", "✅ Yes", "Handed back in the workflow task completion response."),
    ("Eager workflow start", "✅", "❌ No", "SDKs fall back to normal dispatch."),
    ("Worker versioning (build ids, deployments)", "✅", "❌ No", ""),
    ("Completion callbacks and links", "✅", "❌ No", ""),
]:
    p("| " + " | ".join(row) + " |")
p("")

p("## Server features (outside the API)\n")
p("| Feature | Go server | rustify | Notes |")
p("|---|---|---|---|")
for row in [
    ("Persistence", "Cassandra, MySQL, PostgreSQL, SQLite", "SQLite only", "One file; WAL mode."),
    ("Visibility store", "Elasticsearch / OpenSearch, SQL with advanced queries", "Basic SQLite filters", ""),
    (
        "Custom search attributes",
        "✅ Registered per namespace",
        "❌ No",
        "Upserts are stored but can't be registered or queried.",
    ),
    ("Retention clean-up of closed runs", "✅", "❌ No", "Closed runs are kept forever."),
    ("Archival (history and visibility)", "✅ File store, S3, GCS", "❌ No", ""),
    (
        "Scaling out",
        "Many nodes, sharded history, partitioned task queues, membership ring",
        "❌ One process",
        "One lock over all runs.",
    ),
    ("Multi-cluster replication and failover", "✅", "❌ No", ""),
    ("Durability across restarts", "✅", "✅ Yes", "Open runs, timers, queued tasks and retries are restored."),
    ("TLS and mTLS", "✅", "❌ No", "Plain gRPC; bind to localhost or put it behind a proxy."),
    ("Authentication and authorization (JWT, claim mappers, authorizers)", "✅", "❌ No", ""),
    ("Rate limits and quotas", "✅ Per namespace, per task queue", "❌ No", ""),
    ("Dynamic config", "✅ File or service", "❌ No", "Fixed defaults (e.g. 10 s workflow task timeout)."),
    ("Metrics (Prometheus / OpenTelemetry) and tracing", "✅", "❌ No", "Logs only (RUST_LOG)."),
    ("gRPC health service", "✅", "✅ Yes", ""),
    ("gzip compression", "✅", "✅ Yes", "zstd not enabled."),
    ("HTTP/JSON API (grpc-gateway)", "✅", "❌ No", ""),
    ("Temporal Web UI", "✅", "❌ No", "The UI needs many missing operations."),
    (
        "Temporal CLI",
        "✅",
        "🟡 Partial",
        "Workflow start/signal/query/describe/list/terminate/cancel should work; schedules, batch, operator and admin commands don't. Not yet tested.",
    ),
    (
        "Payload and history size limits",
        "✅ Enforced, warns and terminates",
        "✅ Yes",
        "2 MB payloads; 50 MB or 51,200-event histories terminate (Go defaults).",
    ),
    (
        "Official SDK compatibility",
        "All SDKs",
        "🟡 Partial",
        "Python SDK 1.36.1 (23 scenarios) and Go SDK 1.51.0 tested; Java, TypeScript, .NET, Ruby untested.",
    ),
]:
    p("| " + " | ".join(row) + " |")
p("")
p("See `bench/RESULTS.md` for speed and resource use on the operations both servers have.")
print("\n".join(out))
