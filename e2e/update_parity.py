"""Workflow-update parity check: drive a server through update scenarios with raw API calls (no
SDK in between) and print what happened at each step as JSON, so the Go Temporal server and
rustify-temporal can be compared.

Usage: python e2e/update_parity.py HOST:PORT
"""

import asyncio
import json
import sys
import uuid
from datetime import timedelta

from google.protobuf.any_pb2 import Any
from google.protobuf.duration_pb2 import Duration
from temporalio.api.command.v1 import (
    Command,
    CompleteWorkflowExecutionCommandAttributes,
    ContinueAsNewWorkflowExecutionCommandAttributes,
    ProtocolMessageCommandAttributes,
    StartTimerCommandAttributes,
)
from temporalio.api.common.v1 import Payload, Payloads, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import (
    CommandType,
    EventType,
    UpdateWorkflowExecutionLifecycleStage,
    WorkflowTaskFailedCause,
)
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.protocol.v1 import Message
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.update.v1 import (
    Acceptance,
    Input,
    Meta,
    Outcome,
    Rejection,
    Request,
    Response,
    UpdateRef,
    WaitPolicy,
)
from temporalio.api.update.v1 import Request as UpdateRequest
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryRequest,
    PollWorkflowExecutionUpdateRequest,
    PollWorkflowTaskQueueRequest,
    RegisterNamespaceRequest,
    RespondWorkflowTaskCompletedRequest,
    RespondWorkflowTaskFailedRequest,
    SignalWorkflowExecutionRequest,
    StartWorkflowExecutionRequest,
    UpdateWorkflowExecutionRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError

NS = "default"
ACCEPTED = UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
COMPLETED = UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
ADMITTED = UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED


def secs(s: float) -> Duration:
    d = Duration()
    d.FromTimedelta(timedelta(seconds=s))
    return d


def short(event_type: int) -> str:
    return EventType.Name(event_type).replace("EVENT_TYPE_", "")


def payloads(data: bytes = b"x") -> Payloads:
    return Payloads(payloads=[Payload(metadata={"encoding": b"binary/plain"}, data=data)])


def packed(m) -> Any:
    a = Any()
    a.Pack(m)
    return a


def complete_cmd() -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
        complete_workflow_execution_command_attributes=CompleteWorkflowExecutionCommandAttributes(result=payloads()),
    )


def timer_cmd(timer_id: str) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_START_TIMER,
        start_timer_command_attributes=StartTimerCommandAttributes(timer_id=timer_id, start_to_fire_timeout=secs(60)),
    )


def protocol_cmd(message_id: str) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_PROTOCOL_MESSAGE,
        protocol_message_command_attributes=ProtocolMessageCommandAttributes(message_id=message_id),
    )


def accept_msg(req_msg: Message) -> Message:
    return Message(
        id=f"{req_msg.protocol_instance_id}/accept",
        protocol_instance_id=req_msg.protocol_instance_id,
        body=packed(
            Acceptance(
                accepted_request_message_id=req_msg.id,
                accepted_request_sequencing_event_id=req_msg.event_id,
            )
        ),
    )


def reject_msg(req_msg: Message, why: str = "nope") -> Message:
    return Message(
        id=f"{req_msg.protocol_instance_id}/reject",
        protocol_instance_id=req_msg.protocol_instance_id,
        body=packed(
            Rejection(
                rejected_request_message_id=req_msg.id,
                rejected_request_sequencing_event_id=req_msg.event_id,
                failure=Failure(message=why, application_failure_info=ApplicationFailureInfo(type="Rejected")),
            )
        ),
    )


def response_msg(update_id: str, data: bytes = b"done") -> Message:
    return Message(
        id=f"{update_id}/complete",
        protocol_instance_id=update_id,
        body=packed(Response(meta=Meta(update_id=update_id), outcome=Outcome(success=payloads(data)))),
    )


def outcome(o) -> dict:
    if o.HasField("success"):
        return {"success": [p.data.decode() for p in o.success.payloads]}
    if o.HasField("failure"):
        f = o.failure
        return {
            "failure": f.message,
            "type": f.application_failure_info.type if f.HasField("application_failure_info") else None,
        }
    return {}


def message_info(m: Message) -> dict:
    req = UpdateRequest()
    m.body.Unpack(req)
    return {
        "id": m.id,
        "instance": m.protocol_instance_id,
        "type": m.body.type_url.split("/")[-1],
        "name": req.input.name,
        "args": [p.data.decode() for p in req.input.args.payloads],
    }


class Run:
    def __init__(self, client: Client, name: str):
        self.ws = client.workflow_service
        self.wid = f"upd-{name}-{uuid.uuid4().hex[:8]}"
        self.tq = f"tq-{self.wid}"

    async def start(self) -> None:
        await self.ws.start_workflow_execution(
            StartWorkflowExecutionRequest(
                namespace=NS,
                workflow_id=self.wid,
                workflow_type=WorkflowType(name="Raw"),
                task_queue=TaskQueue(name=self.tq),
                workflow_task_timeout=secs(10),
                request_id=str(uuid.uuid4()),
            )
        )

    async def started(self) -> None:
        """Start the run and finish its first workflow task (no commands)."""
        await self.start()
        t = await self.poll()
        await self.complete(t.task_token)

    async def poll(self, timeout: float = 15):
        return await self.ws.poll_workflow_task_queue(
            PollWorkflowTaskQueueRequest(namespace=NS, task_queue=TaskQueue(name=self.tq), identity="parity"),
            timeout=timedelta(seconds=timeout),
        )

    @staticmethod
    def task(t) -> dict:
        evs = list(t.history.events)
        return {
            "attempt": t.attempt,
            "previous_started": t.previous_started_event_id,
            "started": t.started_event_id,
            "events": [short(e.event_type) for e in evs],
            "messages": [message_info(m) for m in t.messages],
            "sequencing": [m.event_id for m in t.messages],
        }

    async def complete(self, token: bytes, commands=(), messages=(), **kw):
        req = RespondWorkflowTaskCompletedRequest(
            namespace=NS,
            task_token=token,
            commands=list(commands),
            messages=list(messages),
            identity="parity",
            **kw,
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

    def update_request(self, update_id: str | None, stage=COMPLETED, name: str = "upd", arg: bytes = b"a", **kw):
        req = Request(
            meta=Meta(update_id=update_id or "", identity="parity"),
            input=Input(name=name, args=payloads(arg)),
        )
        return UpdateWorkflowExecutionRequest(
            namespace=NS,
            workflow_execution=WorkflowExecution(workflow_id=self.wid),
            request=req,
            wait_policy=WaitPolicy(lifecycle_stage=stage),
            **kw,
        )

    async def update(self, update_id: str, stage=COMPLETED, timeout: float = 30, req=None) -> dict:
        try:
            r = await self.ws.update_workflow_execution(
                req or self.update_request(update_id, stage), timeout=timedelta(seconds=timeout)
            )
        except RPCError as e:
            return {"code": e.status.name}
        out = {
            "code": "OK",
            "stage": UpdateWorkflowExecutionLifecycleStage.Name(r.stage).replace(
                "UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_", ""
            ),
            "update_id": r.update_ref.update_id if update_id else bool(r.update_ref.update_id),
            "outcome": outcome(r.outcome) if r.HasField("outcome") else None,
        }
        if r.HasField("link"):
            if r.link.HasField("workflow_event"):
                out["link"] = {
                    "event_id": r.link.workflow_event.event_ref.event_id,
                    "event_type": short(r.link.workflow_event.event_ref.event_type),
                }
            elif r.link.HasField("workflow"):
                out["link"] = {"reason": r.link.workflow.reason}
        return out

    async def poll_update(self, update_id: str, stage=COMPLETED) -> dict:
        try:
            r = await self.ws.poll_workflow_execution_update(
                PollWorkflowExecutionUpdateRequest(
                    namespace=NS,
                    update_ref=UpdateRef(
                        workflow_execution=WorkflowExecution(workflow_id=self.wid), update_id=update_id
                    ),
                    wait_policy=WaitPolicy(lifecycle_stage=stage),
                    identity="parity",
                ),
                timeout=timedelta(seconds=30),
            )
        except RPCError as e:
            return {"code": e.status.name}
        return {
            "code": "OK",
            "stage": UpdateWorkflowExecutionLifecycleStage.Name(r.stage).replace(
                "UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_", ""
            ),
            "outcome": outcome(r.outcome) if r.HasField("outcome") else None,
        }

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

    async def history_events(self):
        h = await self.ws.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(namespace=NS, execution=WorkflowExecution(workflow_id=self.wid))
        )
        return list(h.history.events)

    async def history(self) -> list[str]:
        return [short(e.event_type) for e in await self.history_events()]

    async def update_events(self) -> list[dict]:
        out = []
        for e in await self.history_events():
            if e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
                a = e.workflow_execution_update_accepted_event_attributes
                out.append(
                    {
                        "accepted": a.protocol_instance_id,
                        "message_id": a.accepted_request_message_id,
                        "sequencing": a.accepted_request_sequencing_event_id,
                        "request_name": a.accepted_request.input.name,
                    }
                )
            elif e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
                a = e.workflow_execution_update_completed_event_attributes
                out.append({"completed": a.meta.update_id, "accepted_event": a.accepted_event_id, **outcome(a.outcome)})
        return out


async def later(coro, delay: float = 0.5):
    """Start `coro` now and give it a moment to reach the server."""
    t = asyncio.create_task(coro)
    await asyncio.sleep(delay)
    return t


async def s_accept_complete(c):
    r = Run(c, "accept")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    out = {"task": Run.task(t)}
    m = t.messages[0]
    code, resp = await r.complete(
        t.task_token,
        [protocol_cmd(f"{m.protocol_instance_id}/accept"), protocol_cmd(f"{m.protocol_instance_id}/complete")],
        [accept_msg(m), response_msg(m.protocol_instance_id)],
    )
    out["complete"] = code
    out["reset_history_event_id"] = resp.reset_history_event_id if resp else None
    out["update"] = await u
    out["history"] = await r.history()
    out["update events"] = await r.update_events()
    out["poll update"] = await r.poll_update("u1")
    out["same update again"] = await r.update("u1")
    return out


async def s_reject(c):
    r = Run(c, "reject")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    out = {"task": Run.task(t)}
    code, resp = await r.complete(t.task_token, [], [reject_msg(t.messages[0])])
    out["complete"] = code
    out["reset_history_event_id"] = resp.reset_history_event_id if resp else None
    out["update"] = await u
    out["history"] = await r.history()
    out["poll rejected update"] = await r.poll_update("u1")
    # Rejected updates are forgotten: the same id is a new update.
    u = await later(r.update("u1", timeout=8))
    t = await r.poll()
    out["task for same id again"] = Run.task(t)
    await r.complete(t.task_token, [], [reject_msg(t.messages[0], "again")])
    out["same id again"] = await u
    return out


async def s_accepted_stage(c):
    r = Run(c, "accstage")
    await r.started()
    u = await later(r.update("u1", ACCEPTED))
    t = await r.poll()
    m = t.messages[0]
    out = {"complete": (await r.complete(t.task_token, [protocol_cmd("u1/accept")], [accept_msg(m)]))[0]}
    out["update"] = await u
    out["poll accepted"] = await r.poll_update("u1", ACCEPTED)
    out["history"] = await r.history()
    p = await later(r.poll_update("u1", COMPLETED))
    await r.signal()
    t = await r.poll()
    out["next task"] = Run.task(t)
    out["complete 2"] = (await r.complete(t.task_token, [protocol_cmd("u1/complete")], [response_msg("u1")]))[0]
    out["poll completed"] = await p
    out["history"] = await r.history()
    return out


async def s_unprocessed(c):
    r = Run(c, "unprocessed")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    code, _ = await r.complete(t.task_token, [timer_cmd("t1")])
    out = {"complete ignoring update": code}
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_during_task(c):
    r = Run(c, "during")
    await r.start()
    t = await r.poll()
    u = await later(r.update("u1"))
    code, resp = await r.complete(t.task_token)
    out = {"complete first task": code, "returned task": bool(resp and resp.workflow_task.task_token)}
    out["history after first task"] = await r.history()
    t = await r.poll()
    out["second task"] = Run.task(t)
    m = t.messages[0]
    out["complete"] = (
        await r.complete(
            t.task_token,
            [protocol_cmd("u1/accept"), protocol_cmd("u1/complete"), complete_cmd()],
            [accept_msg(m), response_msg("u1")],
        )
    )[0]
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_return_new_task(c):
    r = Run(c, "returnnew")
    await r.start()
    t = await r.poll()
    u = await later(r.update("u1"))
    code, resp = await r.complete(t.task_token, return_new_workflow_task=True)
    out = {"complete": code, "returned task": Run.task(resp.workflow_task) if resp.workflow_task.task_token else None}
    if resp.workflow_task.task_token:
        t = resp.workflow_task
        code, resp = await r.complete(t.task_token, [], [reject_msg(t.messages[0])], return_new_workflow_task=True)
        out["reject returned task"] = code
        out["reset_history_event_id"] = resp.reset_history_event_id if resp else None
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_closed_workflow(c):
    r = Run(c, "closed")
    await r.start()
    t = await r.poll()
    u = await later(r.update("u1"))
    code, _ = await r.complete(t.task_token, [complete_cmd()])
    out = {"complete workflow with update waiting": code}
    out["waiting update"] = await u
    out["update closed workflow"] = await r.update("u2")
    out["poll unknown update"] = await r.poll_update("nope")
    out["history"] = await r.history()
    return out


async def s_accepted_then_closed(c):
    r = Run(c, "accclosed")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    m = t.messages[0]
    code, _ = await r.complete(t.task_token, [protocol_cmd("u1/accept"), complete_cmd()], [accept_msg(m)])
    out = {"accept and complete workflow": code}
    out["update"] = await u
    out["poll update"] = await r.poll_update("u1")
    out["same update again"] = await r.update("u1")
    out["history"] = await r.history()
    return out


async def s_continue_as_new(c):
    r = Run(c, "can")
    await r.start()
    t = await r.poll()
    u = await later(r.update("u1", timeout=10))
    can = Command(
        command_type=CommandType.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
        continue_as_new_workflow_execution_command_attributes=ContinueAsNewWorkflowExecutionCommandAttributes(
            workflow_type=WorkflowType(name="Raw"), task_queue=TaskQueue(name=r.tq), workflow_task_timeout=secs(10)
        ),
    )
    out = {"continue as new": (await r.complete(t.task_token, [can]))[0]}
    out["update"] = await u
    await asyncio.sleep(1.5)  # the Go server schedules the new run's first task a moment later
    out["new run history"] = await r.history()
    return out


async def s_dedupe(c):
    r = Run(c, "dedupe")
    await r.started()
    u1 = await later(r.update("u1", arg=b"first"))
    u2 = await later(r.update("u1", arg=b"second"))
    t = await r.poll()
    out = {"task": Run.task(t)}
    m = t.messages[0]
    out["complete"] = (
        await r.complete(
            t.task_token,
            [protocol_cmd("u1/accept"), protocol_cmd("u1/complete")],
            [accept_msg(m), response_msg("u1")],
        )
    )[0]
    out["first caller"] = await u1
    out["second caller"] = await u2
    return out


async def s_validation(c):
    r = Run(c, "valid")
    await r.started()
    out = {}
    req = r.update_request("u1")
    req.request.ClearField("meta")
    out["no meta"] = await r.update("u1", req=req)
    req = r.update_request("u1")
    req.request.ClearField("input")
    out["no input"] = await r.update("u1", req=req)
    req = r.update_request("u1", name="")
    out["no name"] = await r.update("u1", req=req)
    out["admitted stage"] = await r.update("u1", ADMITTED)
    req = r.update_request("u1")
    req.workflow_execution.workflow_id = ""
    out["no workflow id"] = await r.update("u1", req=req)
    req = r.update_request("u1")
    req.workflow_execution.run_id = "not-a-uuid"
    out["bad run id"] = await r.update("u1", req=req)
    req = r.update_request("u1", first_execution_run_id=str(uuid.uuid4()))
    out["wrong first run id"] = await r.update("u1", req=req)
    req = r.update_request("u1")
    req.workflow_execution.workflow_id = "no-such-workflow"
    out["unknown workflow"] = await r.update("u1", req=req)
    out["update id too long"] = await r.update("x" * 1001)
    try:
        await r.ws.poll_workflow_execution_update(
            PollWorkflowExecutionUpdateRequest(namespace=NS), timeout=timedelta(seconds=5)
        )
        out["poll without ref"] = "OK"
    except RPCError as e:
        out["poll without ref"] = e.status.name
    # A blank update id gets one from the server.
    u = await later(r.update("", timeout=8))
    t = await r.poll()
    out["blank id task"] = len(t.messages)
    await r.complete(t.task_token, [], [reject_msg(t.messages[0])])
    out["blank id"] = await u
    out["history"] = await r.history()
    return out


async def s_signal_converts(c):
    r = Run(c, "convert")
    await r.started()
    u = await later(r.update("u1"))
    await r.signal()
    out = {"history after signal": await r.history()}
    t = await r.poll()
    out["task"] = Run.task(t)
    out["reject"] = await r.complete(t.task_token, [], [reject_msg(t.messages[0])])
    out["reject"] = (out["reject"][0], out["reject"][1].reset_history_event_id if out["reject"][1] else None)
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_signal_during_speculative(c):
    r = Run(c, "sigdur")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    await r.signal()
    out = {"history while running": await r.history()}
    code, resp = await r.complete(t.task_token, [], [reject_msg(t.messages[0])])
    out["reject"] = code
    out["reset_history_event_id"] = resp.reset_history_event_id if resp else None
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_no_worker(c):
    r = Run(c, "noworker")
    await r.started()
    u = await later(r.update("u1", timeout=25))
    await asyncio.sleep(6.5)  # nobody takes the speculative task: it times out after 5 s
    out = {"history after schedule-to-start": await r.history()}
    t = await r.poll()
    out["task"] = Run.task(t)
    m = t.messages[0]
    out["complete"] = (
        await r.complete(
            t.task_token, [protocol_cmd("u1/accept"), protocol_cmd("u1/complete")], [accept_msg(m), response_msg("u1")]
        )
    )[0]
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_worker_fails_task(c):
    r = Run(c, "wtfail")
    await r.started()
    u = await later(r.update("u1"))
    t = await r.poll()
    out = {"fail": await r.fail(t.task_token)}
    out["history after fail"] = await r.history()
    t = await r.poll()
    out["retry task"] = Run.task(t)
    m = t.messages[0]
    out["complete"] = (
        await r.complete(
            t.task_token, [protocol_cmd("u1/accept"), protocol_cmd("u1/complete")], [accept_msg(m), response_msg("u1")]
        )
    )[0]
    out["update"] = await u
    out["history"] = await r.history()
    return out


async def s_bad_messages(c):
    r = Run(c, "badmsg")
    await r.started()
    u = await later(r.update("u1", timeout=10))
    t = await r.poll()
    # A response for an update that was never accepted.
    out = {"response without acceptance": (await r.complete(t.task_token, [], [response_msg("u1")]))[0]}
    out["update"] = await u
    out["history"] = await r.history()
    t = await r.poll()
    out["next task"] = Run.task(t)
    out["absent message id"] = (await r.complete(t.task_token, [protocol_cmd("missing")]))[0]
    out["history 2"] = await r.history()
    t = await r.poll()
    unknown = Message(
        id="x/accept",
        protocol_instance_id="never-sent",
        body=packed(Acceptance(accepted_request_message_id="x", accepted_request_sequencing_event_id=3)),
    )
    out["accept unknown update"] = (await r.complete(t.task_token, [], [unknown]))[0]
    out["history 3"] = await r.history()
    return out


async def s_in_flight_limit(c):
    r = Run(c, "limit")
    await r.started()
    tasks = []
    for i in range(10):
        tasks.append(await later(r.update(f"u{i}", timeout=25), 0.2))
    out = {"11th update": await r.update("u10", timeout=6)}
    # The Go server may deliver them over several tasks (updates it drops under load are
    # retried internally): reject whatever arrives until all ten are answered.
    for _ in range(10):
        if all(t.done() for t in tasks):
            break
        try:
            t = await r.poll(timeout=3)
        except RPCError:
            continue
        if t.task_token:
            await r.complete(t.task_token, [], [reject_msg(m) for m in t.messages])
    results = await asyncio.gather(*tasks)
    out["all ten"] = sorted(json.dumps(x, sort_keys=True) for x in results)
    out["room for another"] = await r.update("u11", timeout=3)
    return out


async def s_long_poll(c):
    r = Run(c, "longpoll")
    await r.started()
    # Nobody answers within the server's 20 s: an empty answer with the stage reached so far.
    u = await later(r.update("u1", timeout=40))
    t = await r.poll()
    await r.complete(t.task_token, [protocol_cmd("u1/accept")], [accept_msg(t.messages[0])])
    out = {"accepted, waiting for result": await u}
    out["admitted only"] = await r.update("u2", timeout=40)
    out["poll accepted update for result"] = await r.poll_update("u1")
    return out


SCENARIOS = [
    s_accept_complete,
    s_reject,
    s_accepted_stage,
    s_unprocessed,
    s_during_task,
    s_return_new_task,
    s_closed_workflow,
    s_accepted_then_closed,
    s_continue_as_new,
    s_dedupe,
    s_validation,
    s_signal_converts,
    s_signal_during_speculative,
    s_no_worker,
    s_worker_fails_task,
    s_bad_messages,
    s_in_flight_limit,
    s_long_poll,
]


async def ready_namespace(client: Client) -> str:
    """A fresh namespace, usable once the Go server's namespace cache has picked it up."""
    ns = f"upd-parity-{uuid.uuid4().hex[:8]}"
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
