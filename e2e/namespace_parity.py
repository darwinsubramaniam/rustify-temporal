"""Namespace parity check: run the same namespace operations against a server and print what
happened at each step as JSON, so two servers (Go Temporal and rustify-temporal) can be compared.

Usage: python e2e/namespace_parity.py HOST:PORT
"""

import asyncio
import json
import sys
import time
import uuid
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration
from temporalio.api.enums.v1 import NamespaceState, ReplicationState
from temporalio.api.namespace.v1 import (
    BadBinaries,
    BadBinaryInfo,
    NamespaceConfig,
    NamespaceFilter,
    UpdateNamespaceInfo,
)
from temporalio.api.replication.v1 import NamespaceReplicationConfig
from temporalio.api.common.v1 import WorkflowExecution, WorkflowType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    DeprecateNamespaceRequest,
    DescribeNamespaceRequest,
    ListNamespacesRequest,
    PollWorkflowTaskQueueRequest,
    RegisterNamespaceRequest,
    SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest,
    TerminateWorkflowExecutionRequest,
    UpdateNamespaceRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError

HOURS = lambda h: Duration(seconds=int(h * 3600))  # noqa: E731


async def call(fn, req):
    try:
        return "OK", await fn(req)
    except RPCError as e:
        return e.status.name, None


async def eventually(fn, req, want: str, timeout: float = 40):
    """The Go server applies namespace changes through a cache that refreshes every few seconds."""
    deadline = time.time() + timeout
    while True:
        code, resp = await call(fn, req)
        if code == want or time.time() > deadline:
            return code, resp
        await asyncio.sleep(0.5)


async def main(addr: str) -> None:
    c = await Client.connect(addr)
    ws = c.workflow_service
    ns = f"parity-{uuid.uuid4().hex[:8]}"
    out: dict[str, object] = {}

    def rec(step, value):
        out[step] = value

    rec("register without retention", (await call(ws.register_namespace, RegisterNamespaceRequest(namespace=ns)))[0])
    rec(
        "register with 30 min retention",
        (
            await call(
                ws.register_namespace,
                RegisterNamespaceRequest(namespace=ns, workflow_execution_retention_period=HOURS(0.5)),
            )
        )[0],
    )
    rec(
        "register global namespace",
        (
            await call(
                ws.register_namespace,
                RegisterNamespaceRequest(
                    namespace=ns, workflow_execution_retention_period=HOURS(24), is_global_namespace=True
                ),
            )
        )[0],
    )
    rec(
        "register unknown active cluster",
        (
            await call(
                ws.register_namespace,
                RegisterNamespaceRequest(
                    namespace=ns, workflow_execution_retention_period=HOURS(24), active_cluster_name="nope"
                ),
            )
        )[0],
    )
    rec(
        "register without name",
        (await call(ws.register_namespace, RegisterNamespaceRequest(workflow_execution_retention_period=HOURS(24))))[0],
    )
    rec(
        "register",
        (
            await call(
                ws.register_namespace,
                RegisterNamespaceRequest(
                    namespace=ns,
                    description="d1",
                    owner_email="o@example.com",
                    workflow_execution_retention_period=HOURS(24),
                    data={"k1": "v1"},
                ),
            )
        )[0],
    )
    rec(
        "register again",
        (
            await call(
                ws.register_namespace,
                RegisterNamespaceRequest(namespace=ns, workflow_execution_retention_period=HOURS(24)),
            )
        )[0],
    )

    code, d = await call(ws.describe_namespace, DescribeNamespaceRequest(namespace=ns))
    rec(
        "describe",
        {
            "code": code,
            "state": NamespaceState.Name(d.namespace_info.state),
            "description": d.namespace_info.description,
            "owner": d.namespace_info.owner_email,
            "data": dict(sorted(d.namespace_info.data.items())),
            "retention_h": d.config.workflow_execution_retention_ttl.seconds / 3600,
            "global": d.is_global_namespace,
            "clusters": [
                x.cluster_name == d.replication_config.active_cluster_name for x in d.replication_config.clusters
            ],
        },
    )
    code2, d2 = await call(ws.describe_namespace, DescribeNamespaceRequest(id=d.namespace_info.id))
    rec("describe by id", [code2, d2.namespace_info.name == ns if d2 else None])
    rec(
        "describe missing",
        (await call(ws.describe_namespace, DescribeNamespaceRequest(namespace="no-such-namespace")))[0],
    )
    rec("describe without name or id", (await call(ws.describe_namespace, DescribeNamespaceRequest()))[0])

    # Paging: one namespace per page until the token runs out.
    names, token, pages = [], b"", 0
    while True:
        code, page = await call(ws.list_namespaces, ListNamespacesRequest(page_size=1, next_page_token=token))
        if code != "OK":
            break
        pages += 1
        names += [x.namespace_info.name for x in page.namespaces]
        token = page.next_page_token
        if not token or pages > 1000:
            break
    rec(
        "list one per page",
        {
            "code": code,
            "found": ns in names,
            "several pages": pages > 1,
            "each page one": len(names) == pages or len(names) == pages - 1,
        },
    )

    upd = lambda **kw: ws.update_namespace(UpdateNamespaceRequest(namespace=ns, **kw))  # noqa: E731
    code, u = await call(
        lambda _: upd(
            update_info=UpdateNamespaceInfo(description="d2", data={"k2": "v2"}),
            config=NamespaceConfig(
                bad_binaries=BadBinaries(binaries={"bin-1": BadBinaryInfo(reason="broken", operator="me")})
            ),
        ),
        None,
    )
    rec(
        "update description, data, bad binary",
        {
            "code": code,
            "description": u.namespace_info.description if u else None,
            "data": dict(sorted(u.namespace_info.data.items())) if u else None,
            "bad binary": sorted(u.config.bad_binaries.binaries.keys()) if u else None,
            "bad binary has time": bool(u.config.bad_binaries.binaries["bin-1"].create_time.seconds) if u else None,
        },
    )
    poll = PollWorkflowTaskQueueRequest(
        namespace=ns, task_queue=TaskQueue(name="parity-poll"), identity="parity", binary_checksum="bin-1"
    )
    rec(
        "poll from a build marked bad",
        (
            await eventually(
                lambda r: ws.poll_workflow_task_queue(r, timeout=timedelta(seconds=5)),
                poll,
                "INVALID_ARGUMENT",
                timeout=40,
            )
        )[0],
    )
    rec("update delete unknown bad binary", (await call(lambda _: upd(delete_bad_binary="nope"), None))[0])
    code, u = await call(lambda _: upd(delete_bad_binary="bin-1"), None)
    rec("update delete bad binary", [code, sorted(u.config.bad_binaries.binaries.keys()) if u else None])
    many = BadBinaries(binaries={f"b{i}": BadBinaryInfo(reason="x") for i in range(11)})
    rec(
        "update more than 10 bad binaries",
        (await call(lambda _: upd(config=NamespaceConfig(bad_binaries=many)), None))[0],
    )
    rec(
        "update retention 10 min",
        (await call(lambda _: upd(config=NamespaceConfig(workflow_execution_retention_ttl=HOURS(1 / 6))), None))[0],
    )
    code, u = await call(lambda _: upd(config=NamespaceConfig(workflow_execution_retention_ttl=HOURS(48))), None)
    rec("update retention 48 h", [code, u.config.workflow_execution_retention_ttl.seconds / 3600 if u else None])
    rec("update promote to global", (await call(lambda _: upd(promote_namespace=True), None))[0])
    rec(
        "update replication handover",
        (
            await call(
                lambda _: upd(
                    replication_config=NamespaceReplicationConfig(state=ReplicationState.REPLICATION_STATE_HANDOVER)
                ),
                None,
            )
        )[0],
    )
    rec(
        "update unknown active cluster",
        (await call(lambda _: upd(replication_config=NamespaceReplicationConfig(active_cluster_name="nope")), None))[0],
    )
    rec(
        "update missing namespace",
        (
            await call(
                ws.update_namespace,
                UpdateNamespaceRequest(namespace="no-such-namespace", update_info=UpdateNamespaceInfo(description="x")),
            )
        )[0],
    )

    # A workflow started before deprecation keeps running; new ones can't start.
    start = lambda wid: StartWorkflowExecutionRequest(  # noqa: E731
        namespace=ns,
        workflow_id=wid,
        workflow_type=WorkflowType(name="Anything"),
        task_queue=TaskQueue(name="parity"),
        request_id=str(uuid.uuid4()),
    )
    rec("start workflow while registered", (await eventually(ws.start_workflow_execution, start("before"), "OK"))[0])
    rec("deprecate", (await call(ws.deprecate_namespace, DeprecateNamespaceRequest(namespace=ns)))[0])
    code, d = await call(ws.describe_namespace, DescribeNamespaceRequest(namespace=ns))
    rec("describe after deprecate", NamespaceState.Name(d.namespace_info.state) if d else code)
    rec(
        "start workflow while deprecated",
        (await eventually(ws.start_workflow_execution, start("after"), "FAILED_PRECONDITION"))[0],
    )
    rec(
        "signal running workflow while deprecated",
        (
            await call(
                ws.signal_workflow_execution,
                SignalWorkflowExecutionRequest(
                    namespace=ns,
                    workflow_execution=WorkflowExecution(workflow_id="before"),
                    signal_name="s",
                    request_id=str(uuid.uuid4()),
                ),
            )
        )[0],
    )
    rec(
        "terminate running workflow while deprecated",
        (
            await call(
                ws.terminate_workflow_execution,
                TerminateWorkflowExecutionRequest(
                    namespace=ns, workflow_execution=WorkflowExecution(workflow_id="before"), reason="done"
                ),
            )
        )[0],
    )
    rec("deprecate again", (await call(ws.deprecate_namespace, DeprecateNamespaceRequest(namespace=ns)))[0])
    rec(
        "update deprecated back to registered",
        (
            await call(
                lambda _: upd(update_info=UpdateNamespaceInfo(state=NamespaceState.NAMESPACE_STATE_REGISTERED)), None
            )
        )[0],
    )
    rec(
        "deprecate missing namespace",
        (await call(ws.deprecate_namespace, DeprecateNamespaceRequest(namespace="no-such-namespace")))[0],
    )
    rec("deprecate without name", (await call(ws.deprecate_namespace, DeprecateNamespaceRequest()))[0])

    rec(
        "update deprecated to deleted",
        (
            await call(
                lambda _: upd(update_info=UpdateNamespaceInfo(state=NamespaceState.NAMESPACE_STATE_DELETED)), None
            )
        )[0],
    )

    async def listed(include_deleted: bool) -> bool:
        found, token = [], b""
        while True:
            _, page = await call(
                ws.list_namespaces,
                ListNamespacesRequest(
                    page_size=100,
                    next_page_token=token,
                    namespace_filter=NamespaceFilter(include_deleted=include_deleted),
                ),
            )
            found += [x.namespace_info.name for x in page.namespaces]
            token = page.next_page_token
            if not token:
                return ns in found

    rec("list hides deleted", not await listed(False))
    rec("list with include_deleted shows it", await listed(True))
    code, d = await call(ws.describe_namespace, DescribeNamespaceRequest(namespace=ns))
    rec("describe deleted", NamespaceState.Name(d.namespace_info.state) if d else code)
    rec(
        "update deleted namespace",
        (
            await eventually(
                lambda r: upd(update_info=UpdateNamespaceInfo(description="x")), None, "FAILED_PRECONDITION"
            )
        )[0],
    )
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
