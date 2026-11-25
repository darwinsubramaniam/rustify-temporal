"""Workflow-task parity check: drive a server through workflow-task scenarios with raw API calls
(no SDK in between) and print what happened at each step as JSON, so the Go Temporal server and
rustify-temporal can be compared.

Usage: python e2e/wft_parity.py HOST:PORT
"""

import asyncio
import json
import sys
import uuid
from datetime import timedelta

from google.protobuf.duration_pb2 import Duration
from temporalio.api.command.v1 import (
    Command,
    CompleteWorkflowExecutionCommandAttributes,
    RequestCancelActivityTaskCommandAttributes,
    ScheduleActivityTaskCommandAttributes,
)
from temporalio.api.common.v1 import ActivityType, Payload, Payloads, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import CommandType, EventType, TaskQueueKind, WorkflowTaskFailedCause
from temporalio.api.taskqueue.v1 import StickyExecutionAttributes, TaskQueue
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionRequest,
    GetWorkflowExecutionHistoryRequest,
    PollWorkflowTaskQueueRequest,
    ResetStickyTaskQueueRequest,
    RespondActivityTaskCompletedRequest,
    RespondWorkflowTaskCompletedRequest,
    RespondWorkflowTaskFailedRequest,
    RegisterNamespaceRequest,
    SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError

NS = "default"


def secs(s: float) -> Duration:
    d = Duration()
    d.FromTimedelta(timedelta(seconds=s))
    return d


def short(event_type: int) -> str:
    return EventType.Name(event_type).replace("EVENT_TYPE_", "")


def payloads(size: int = 3) -> Payloads:
    return Payloads(payloads=[Payload(metadata={"encoding": b"binary/plain"}, data=b"x" * size)])


def complete_cmd(size: int = 3) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
        complete_workflow_execution_command_attributes=CompleteWorkflowExecutionCommandAttributes(
            result=payloads(size)
        ),
    )


def activity_cmd(activity_id: str, tq: str, eager: bool = False, with_type: bool = True) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        schedule_activity_task_command_attributes=ScheduleActivityTaskCommandAttributes(
            activity_id=activity_id,
            activity_type=ActivityType(name="act") if with_type else None,
            task_queue=TaskQueue(name=tq),
            start_to_close_timeout=secs(30),
            request_eager_execution=eager,
        ),
    )


class Run:
    def __init__(self, client: Client, name: str):
        self.ws = client.workflow_service
        self.wid = f"wft-{name}-{uuid.uuid4().hex[:8]}"
        self.tq = f"tq-{self.wid}"

    async def start(self, wft_timeout: float = 10) -> None:
        await self.ws.start_workflow_execution(
            StartWorkflowExecutionRequest(
                namespace=NS,
                workflow_id=self.wid,
                workflow_type=WorkflowType(name="Raw"),
                task_queue=TaskQueue(name=self.tq),
                workflow_task_timeout=secs(wft_timeout),
                request_id=str(uuid.uuid4()),
            )
        )

    async def poll(self, sticky: str | None = None, timeout: float = 15):
        tq = (
            TaskQueue(name=sticky, kind=TaskQueueKind.TASK_QUEUE_KIND_STICKY, normal_name=self.tq)
            if sticky
            else TaskQueue(name=self.tq)
        )
        return await self.ws.poll_workflow_task_queue(
            PollWorkflowTaskQueueRequest(namespace=NS, task_queue=tq, identity="parity"),
            timeout=timedelta(seconds=timeout),
        )

    @staticmethod
    def task(t) -> dict:
        evs = list(t.history.events)
        return {
            "attempt": t.attempt,
            "previous_started": t.previous_started_event_id,
            "started": t.started_event_id,
            "first_event": evs[0].event_id if evs else None,
            "events": [short(e.event_type) for e in evs],
        }

    async def complete(self, token: bytes, commands=(), sticky: str | None = None, **kw):
        req = RespondWorkflowTaskCompletedRequest(
            namespace=NS, task_token=token, commands=list(commands), identity="parity", **kw
        )
        if sticky:
            req.sticky_attributes.CopyFrom(
                StickyExecutionAttributes(worker_task_queue=TaskQueue(name=sticky), schedule_to_start_timeout=secs(1))
            )
        try:
            return "OK", await self.ws.respond_workflow_task_completed(req)
        except RPCError as e:
            return e.status.name, None

    async def fail(self, token: bytes) -> str:
        try:
            await self.ws.respond_workflow_task_failed(
                RespondWorkflowTaskFailedRequest(
                    namespace=NS,
                    task_token=token,
                    cause=WorkflowTaskFailedCause.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
                    identity="parity",
                )
            )
            return "OK"
        except RPCError as e:
            return e.status.name

    async def signal(self) -> None:
        await self.ws.signal_workflow_execution(
            SignalWorkflowExecutionRequest(
                namespace=NS,
                workflow_execution=WorkflowExecution(workflow_id=self.wid),
                signal_name="s",
                identity="parity",
                request_id=str(uuid.uuid4()),
            )
        )

    async def history(self) -> list[str]:
        h = await self.ws.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(namespace=NS, execution=WorkflowExecution(workflow_id=self.wid))
        )
        return [short(e.event_type) for e in h.history.events]

    async def history_events(self):
        h = await self.ws.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(namespace=NS, execution=WorkflowExecution(workflow_id=self.wid))
        )
        return list(h.history.events)

    async def describe(self):
        return await self.ws.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(namespace=NS, execution=WorkflowExecution(workflow_id=self.wid))
        )


async def s_basic(c):
    r = Run(c, "basic")
    await r.start()
    t = await r.poll()
    out = {"task": Run.task(t), "queries": len(t.queries)}
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    out["status"] = (await r.describe()).workflow_execution_info.status
    return out


async def s_fail_retries(c):
    r = Run(c, "fail")
    await r.start()
    out = {}
    for i in range(1, 4):
        t = await r.poll()
        out[f"poll {i}"] = Run.task(t)
        out[f"fail {i}"] = await r.fail(t.task_token)
        out[f"history after fail {i}"] = await r.history()
    t = await r.poll()
    out["poll 4"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    evs = await r.history_events()
    out["history"] = [short(e.event_type) for e in evs]
    out["scheduled attempts"] = [
        e.workflow_task_scheduled_event_attributes.attempt
        for e in evs
        if e.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    ]
    return out


async def s_fail_then_signal(c):
    r = Run(c, "failsignal")
    await r.start()
    t = await r.poll()
    out = {"fail": await r.fail(t.task_token)}
    t = await r.poll()
    out["fail 2"] = await r.fail(t.task_token)
    await r.signal()
    out["history after signal"] = await r.history()
    t = await r.poll()
    out["poll after signal"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_timeout(c):
    r = Run(c, "timeout")
    await r.start(wft_timeout=1)
    t = await r.poll()
    out = {"poll 1": Run.task(t)}
    await asyncio.sleep(2.5)
    out["history after timeout"] = await r.history()
    t = await r.poll()
    out["poll 2"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_unhandled(c):
    r = Run(c, "unhandled")
    await r.start()
    t = await r.poll()
    await r.signal()
    out = {"complete while signal waits": (await r.complete(t.task_token, [complete_cmd()]))[0]}
    t = await r.poll()
    out["next poll"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_bad_command(c):
    r = Run(c, "badcmd")
    await r.start()
    t = await r.poll()
    out = {"bad command": (await r.complete(t.task_token, [activity_cmd("a1", r.tq, with_type=False)]))[0]}
    out["history after bad command"] = await r.history()
    t = await r.poll()
    out["next poll"] = Run.task(t)
    out["bad command again"] = (await r.complete(t.task_token, [activity_cmd("a1", r.tq, with_type=False)]))[0]
    out["history after second bad command"] = await r.history()
    return out


async def s_sticky(c):
    r = Run(c, "sticky")
    sticky = f"{r.tq}-sticky"
    await r.start()
    t = await r.poll()
    out = {"complete with sticky": (await r.complete(t.task_token, [], sticky=sticky))[0]}
    # Poll the sticky queue before the signal, as a real worker does: the Go server only sends
    # tasks to a sticky queue that has a poller (otherwise they go to the normal queue).
    p = asyncio.create_task(r.poll(sticky))
    await asyncio.sleep(0.5)
    await r.signal()
    t = await p
    out["sticky poll"] = Run.task(t)
    out["complete on sticky"] = (await r.complete(t.task_token, [], sticky=sticky))[0]
    await r.signal()
    await asyncio.sleep(3)  # nobody polls the sticky queue: it times out (1 s)
    out["history after sticky timeout"] = await r.history()
    t = await r.poll()
    out["normal poll"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_reset_sticky(c):
    r = Run(c, "reset")
    sticky = f"{r.tq}-sticky"
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [], sticky=sticky)
    await r.signal()
    await r.ws.reset_sticky_task_queue(
        ResetStickyTaskQueueRequest(namespace=NS, execution=WorkflowExecution(workflow_id=r.wid))
    )
    t = await r.poll(timeout=8)
    out = {"normal poll after reset": Run.task(t)}
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_tokens(c):
    r = Run(c, "tokens")
    await r.start()
    t = await r.poll()
    out = {"garbage token": (await r.complete(b"not-a-token", []))[0]}
    out["complete"] = (await r.complete(t.task_token, []))[0]
    out["complete same task again"] = (await r.complete(t.task_token, []))[0]
    out["fail completed task"] = await r.fail(t.task_token)
    return out


async def s_heartbeat(c):
    r = Run(c, "heartbeat")
    await r.start()
    t = await r.poll()
    code, resp = await r.complete(t.task_token, [], force_create_new_workflow_task=True, return_new_workflow_task=True)
    out = {
        "complete with new task": code,
        "returned task": Run.task(resp.workflow_task) if resp.workflow_task.task_token else None,
    }
    if resp.workflow_task.task_token:
        out["complete returned task"] = (await r.complete(resp.workflow_task.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_eager_activity(c):
    r = Run(c, "eager")
    await r.start()
    t = await r.poll()
    code, resp = await r.complete(t.task_token, [activity_cmd("a1", r.tq, eager=True)])
    out = {"complete": code, "eager activities": len(resp.activity_tasks) if resp else None}
    if resp and resp.activity_tasks:
        a = resp.activity_tasks[0]
        out["eager activity"] = {"id": a.activity_id, "attempt": a.attempt, "type": a.activity_type.name}
        await r.ws.respond_activity_task_completed(
            RespondActivityTaskCompletedRequest(
                namespace=NS, task_token=a.task_token, result=payloads(), identity="parity"
            )
        )
    out["history after activity"] = await r.history()
    t = await r.poll()
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["history"] = await r.history()
    return out


async def s_cancel_unstarted_activity(c):
    r = Run(c, "cancelact")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [activity_cmd("a1", f"{r.tq}-nobody")])
    scheduled = [
        e.event_id for e in await r.history_events() if e.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    ][0]
    await r.signal()
    t = await r.poll()
    cancel = Command(
        command_type=CommandType.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
        request_cancel_activity_task_command_attributes=RequestCancelActivityTaskCommandAttributes(
            scheduled_event_id=scheduled
        ),
    )
    out = {"cancel": (await r.complete(t.task_token, [cancel]))[0]}
    out["history after cancel"] = await r.history()
    d = await r.describe()
    out["workflow task pending"] = d.HasField("pending_workflow_task")
    return out


async def s_payload_limit(c):
    r = Run(c, "bigresult")
    await r.start()
    t = await r.poll()
    out = {"complete with 2.1 MB result": (await r.complete(t.task_token, [complete_cmd(2_200_000)]))[0]}
    out["history"] = await r.history()
    out["status"] = (await r.describe()).workflow_execution_info.status
    return out


SCENARIOS = [
    s_basic,
    s_fail_retries,
    s_fail_then_signal,
    s_timeout,
    s_unhandled,
    s_bad_command,
    s_sticky,
    s_reset_sticky,
    s_tokens,
    s_heartbeat,
    s_eager_activity,
    s_cancel_unstarted_activity,
    s_payload_limit,
]


async def ready_namespace(client: Client) -> str:
    """A fresh namespace, usable once the Go server's namespace cache has picked it up."""
    ns = f"wft-parity-{uuid.uuid4().hex[:8]}"
    await client.workflow_service.register_namespace(
        RegisterNamespaceRequest(namespace=ns, workflow_execution_retention_period=secs(24 * 3600))
    )
    for _ in range(120):
        try:
            await client.workflow_service.start_workflow_execution(
                StartWorkflowExecutionRequest(
                    namespace=ns,
                    workflow_id="probe",
                    workflow_type=WorkflowType(name="Probe"),
                    task_queue=TaskQueue(name="probe"),
                    request_id=str(uuid.uuid4()),
                )
            )
            return ns
        except RPCError:
            await asyncio.sleep(0.5)
    raise RuntimeError("namespace never became usable")


async def main(addr: str) -> None:
    global NS
    c = await Client.connect(addr)
    NS = await ready_namespace(c)
    out = {}
    results = await asyncio.gather(*[s(c) for s in SCENARIOS], return_exceptions=True)
    for s, res in zip(SCENARIOS, results):
        name = s.__name__[2:]
        if isinstance(res, BaseException):
            out[name] = {"error": f"{type(res).__name__}: {res}"}
        else:
            for k, v in res.items():
                out[f"{name} / {k}"] = v
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
