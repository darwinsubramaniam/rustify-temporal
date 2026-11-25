"""Running-workflow parity check: history paging (forward and reverse) and the other
running-workflow operations, driven with raw API calls and printed as JSON so the Go Temporal
server and rustify-temporal can be compared.

Usage: python e2e/running_parity.py HOST:PORT
"""

import asyncio
import json
import re
import sys
import uuid
from datetime import timedelta
from pathlib import Path

from google.protobuf.field_mask_pb2 import FieldMask
from temporalio.api.command.v1 import (
    Command,
    ScheduleActivityTaskCommandAttributes,
    StartChildWorkflowExecutionCommandAttributes,
    StartTimerCommandAttributes,
)
from temporalio.api.common.v1 import ActivityType, Priority, TimeSkippingConfig, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import (
    CommandType,
    HistoryEventFilterType,
    ResetReapplyType,
    UpdateAdmittedEventOrigin,
    WorkflowIdReusePolicy,
    WorkflowTaskFailedCause,
)
from temporalio.api.enums.v1 import ResetReapplyExcludeType as RESET_EXCLUDE
from temporalio.api.enums.v1 import WorkflowIdConflictPolicy as CONFLICT
from temporalio.api.errordetails.v1 import MultiOperationExecutionFailure
from temporalio.api.protocol.v1 import Message
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflow.v1 import PostResetOperation, WorkflowExecutionOptions
from temporalio.api.workflowservice.v1 import (
    DeleteWorkflowExecutionRequest,
    DescribeWorkflowExecutionRequest,
    ExecuteMultiOperationRequest,
    GetWorkflowExecutionHistoryRequest,
    GetWorkflowExecutionHistoryReverseRequest,
    PauseWorkflowExecutionRequest,
    PollActivityTaskQueueRequest,
    ResetWorkflowExecutionRequest,
    RespondActivityTaskCompletedRequest,
    StartWorkflowExecutionRequest,
    TerminateWorkflowExecutionRequest,
    UnpauseWorkflowExecutionRequest,
    UpdateWorkflowExecutionOptionsRequest,
)
from temporalio.client import Client
from temporalio.service import RPCError

sys.path.insert(0, str(Path(__file__).parent))
import update_parity as up
import wft_parity as wp
from wft_parity import Run, complete_cmd, payloads, ready_namespace, secs, short


def ns() -> str:
    return wp.NS


def timer_cmd(timer_id: str, seconds: float = 60) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_START_TIMER,
        start_timer_command_attributes=StartTimerCommandAttributes(
            timer_id=timer_id, start_to_fire_timeout=secs(seconds)
        ),
    )


def activity_cmd(activity_id: str, tq: str) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
        schedule_activity_task_command_attributes=ScheduleActivityTaskCommandAttributes(
            activity_id=activity_id,
            activity_type=ActivityType(name="act"),
            task_queue=TaskQueue(name=tq),
            start_to_close_timeout=secs(30),
        ),
    )


def child_cmd(workflow_id: str, tq: str) -> Command:
    return Command(
        command_type=CommandType.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
        start_child_workflow_execution_command_attributes=StartChildWorkflowExecutionCommandAttributes(
            workflow_id=workflow_id,
            workflow_type=WorkflowType(name="Child"),
            task_queue=TaskQueue(name=tq),
            workflow_task_timeout=secs(10),
        ),
    )


async def get_page(r: Run, size: int = 0, token: bytes = b"", **kw):
    try:
        h = await r.ws.get_workflow_execution_history(
            GetWorkflowExecutionHistoryRequest(
                namespace=ns(),
                execution=WorkflowExecution(workflow_id=r.wid, run_id=kw.pop("run_id", "")),
                maximum_page_size=size,
                next_page_token=token,
                **kw,
            ),
            timeout=timedelta(seconds=30),
        )
    except RPCError as e:
        return e.status.name, None
    return [short(e.event_type) for e in h.history.events], h.next_page_token


async def pages(r: Run, size: int, reverse: bool = False, limit: int = 40, **kw) -> list:
    """Every page until the token runs out: each page's event types, plus whether it ended."""
    out, token = [], b""
    for _ in range(limit):
        if reverse:
            try:
                h = await r.ws.get_workflow_execution_history_reverse(
                    GetWorkflowExecutionHistoryReverseRequest(
                        namespace=ns(),
                        execution=WorkflowExecution(workflow_id=r.wid),
                        maximum_page_size=size,
                        next_page_token=token,
                    )
                )
            except RPCError as e:
                out.append(e.status.name)
                return out
            evs, token = [short(e.event_type) for e in h.history.events], h.next_page_token
        else:
            evs, token = await get_page(r, size, token, **kw)
            if token is None:
                out.append(evs)
                return out
        out.append(evs)
        if not token:
            return out
    out.append("…")
    return out


async def busy_run(c, name: str) -> Run:
    """A run with several batches: tasks, a signal while a task runs, an activity, a timer."""
    r = Run(c, name)
    await r.start()
    t = await r.poll()
    await r.signal()  # arrives while the task runs: written with the completion
    await r.complete(t.task_token, [activity_cmd("a1", r.tq), timer_cmd("t1")])
    a = await r.ws.poll_activity_task_queue(
        PollActivityTaskQueueRequest(namespace=ns(), task_queue=TaskQueue(name=r.tq), identity="parity"),
        timeout=timedelta(seconds=15),
    )
    await r.ws.respond_activity_task_completed(
        RespondActivityTaskCompletedRequest(namespace=ns(), task_token=a.task_token, result=payloads(), identity="p")
    )
    t = await r.poll()
    await r.complete(t.task_token, [])
    await r.signal()
    await r.signal()
    return r


async def s_paging(c):
    r = await busy_run(c, "paging")
    out = {}
    for size in (1, 2, 3, 0):
        out[f"running, page size {size}"] = await pages(r, size)
    t = await r.poll()
    await r.complete(t.task_token, [complete_cmd()])
    for size in (1, 2, 4, 0):
        out[f"closed, page size {size}"] = await pages(r, size)
    for size in (1, 3, 0):
        out[f"reverse, page size {size}"] = await pages(r, size, reverse=True)
    out["close event only"] = await pages(
        r, 1, history_event_filter_type=HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    )
    return out


async def s_transient_last_page(c):
    r = Run(c, "transient")
    await r.start()
    for _ in range(2):
        t = await r.poll()
        await r.fail(t.task_token)
    out = {"pages of 1": await pages(r, 1)}
    out["pages of 2"] = await pages(r, 2)
    out["reverse"] = await pages(r, 2, reverse=True)
    return out


async def s_long_poll(c):
    r = Run(c, "longpoll")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [timer_cmd("t1")])
    first, token = await get_page(r, 2, wait_new_event=True)
    out = {"first page": first, "token": bool(token)}
    second, token = await get_page(r, 2, token, wait_new_event=True)
    out["second page"] = second
    waiting = asyncio.create_task(get_page(r, 2, token, wait_new_event=True))
    await asyncio.sleep(1)
    await r.signal()
    evs, token = await waiting
    out["after signal"] = evs
    t = await r.poll()
    await r.complete(t.task_token, [complete_cmd()])
    evs, token = await get_page(r, 0, token, wait_new_event=True)
    out["after completion"] = evs
    out["token after close"] = bool(token)
    # Without waiting, a running run's history ends without a token.
    r2 = Run(c, "nowait")
    await r2.start()
    evs, token = await get_page(r2, 0)
    out["running, no wait"] = (evs, bool(token))
    return out


async def s_children(c):
    r = Run(c, "parent")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [child_cmd(f"{r.wid}-child", f"{r.tq}-child")])
    await asyncio.sleep(1)  # the child starts and the parent hears of it
    return {"parent pages of 1": await pages(r, 1)}


async def s_errors(c):
    r = Run(c, "histerr")
    await r.start()
    out = {"bad token": (await get_page(r, 1, b"garbage"))[0]}
    _, token = await get_page(r, 1)
    out["run id mismatch"] = (await get_page(r, 1, token, run_id=str(uuid.uuid4())))[0]
    out["bad run id"] = (await get_page(r, 1, run_id="nope"))[0]
    try:
        await r.ws.get_workflow_execution_history(GetWorkflowExecutionHistoryRequest(namespace=ns()))
        out["no execution"] = "OK"
    except RPCError as e:
        out["no execution"] = e.status.name
    try:
        await r.ws.get_workflow_execution_history_reverse(
            GetWorkflowExecutionHistoryReverseRequest(namespace=ns(), execution=WorkflowExecution(workflow_id="nope"))
        )
        out["reverse unknown workflow"] = "OK"
    except RPCError as e:
        out["reverse unknown workflow"] = e.status.name
    return out


async def call(coro) -> str:
    try:
        await coro
        return "OK"
    except RPCError as e:
        return e.status.name


async def delete(r: Run, run_id: str = "") -> str:
    return await call(
        r.ws.delete_workflow_execution(
            DeleteWorkflowExecutionRequest(
                namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid, run_id=run_id)
            )
        )
    )


async def describe(r: Run, run_id: str = "") -> dict:
    try:
        d = await r.ws.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(
                namespace=ns(), execution=WorkflowExecution(workflow_id=r.wid, run_id=run_id)
            )
        )
    except RPCError as e:
        return {"code": e.status.name}
    return {"code": "OK", "status": d.workflow_execution_info.status}


async def start(r: Run, policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE) -> str:
    resp = await r.ws.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=ns(),
            workflow_id=r.wid,
            workflow_type=WorkflowType(name="Raw"),
            task_queue=TaskQueue(name=r.tq),
            workflow_task_timeout=secs(10),
            request_id=str(uuid.uuid4()),
            workflow_id_reuse_policy=policy,
        )
    )
    return resp.run_id


async def gone(r: Run, run_id: str = "") -> bool:
    """Wait for a deleted run to disappear (the Go server removes closed runs about 30 s later)."""
    for _ in range(90):
        if (await describe(r, run_id))["code"] == "NOT_FOUND":
            return True
        await asyncio.sleep(1)
    return False


async def s_delete(c):
    out = {}
    # A closed run.
    r = Run(c, "delclosed")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [complete_cmd()])
    out["delete closed"] = await delete(r)
    out["closed run gone"] = await gone(r)
    out["history after"] = (await get_page(r))[0]
    out["delete again"] = await delete(r)
    out["start same id, reject duplicate"] = await call(
        start(r, WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
    )
    out["start same id, allow duplicate"] = await call(start(r))
    # A running run is terminated, then removed.
    r = Run(c, "delrunning")
    await r.start()
    out["delete running"] = await delete(r)
    out["describe right after"] = await describe(r)
    await asyncio.sleep(2)
    out["describe later"] = await describe(r)
    out["poll its task"] = Run.task(await r.poll(timeout=3))
    # An older run of a workflow id; the current one stays.
    r = Run(c, "delold")
    first = await start(r)
    await r.ws.terminate_workflow_execution(
        TerminateWorkflowExecutionRequest(namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid))
    )
    await start(r)
    out["delete older run"] = await delete(r, first)
    out["older run gone"] = await gone(r, first)
    out["describe current run"] = await describe(r)
    out["delete unknown"] = await delete(Run(c, "nobody"))
    out["delete bad run id"] = await delete(r, "nope")
    return out


def uws_request(r: Run, update_id: str, **start_kw) -> ExecuteMultiOperationRequest:
    upd = up.Run.update_request(r, update_id)
    upd.namespace = ns()
    start = StartWorkflowExecutionRequest(
        namespace=ns(),
        workflow_id=r.wid,
        workflow_type=WorkflowType(name="Raw"),
        task_queue=TaskQueue(name=r.tq),
        workflow_task_timeout=secs(10),
        request_id=start_kw.pop("request_id", str(uuid.uuid4())),
        **start_kw,
    )
    return ExecuteMultiOperationRequest(
        namespace=ns(),
        operations=[
            ExecuteMultiOperationRequest.Operation(start_workflow=start),
            ExecuteMultiOperationRequest.Operation(update_workflow=upd),
        ],
    )


def link_info(link) -> dict | None:
    if link.HasField("workflow_event"):
        return {
            "event_id": link.workflow_event.event_ref.event_id,
            "type": short(link.workflow_event.event_ref.event_type),
        }
    return None


async def uws(r: Run, req) -> dict:
    try:
        resp = await r.ws.execute_multi_operation(req, timeout=timedelta(seconds=30))
    except RPCError as e:
        out = {"code": e.status.name, "message": e.message}
        if e.grpc_status.details:
            f = MultiOperationExecutionFailure()
            if e.grpc_status.details[0].Unpack(f):
                for st in f.statuses:  # ids differ between servers
                    st.message = re.sub(r"RunId: [0-9a-f-]+", "RunId: <run>", st.message.replace(r.wid, "<wid>"))
                out["statuses"] = [
                    (st.code, st.message, [d.type_url.split("/")[-1] for d in st.details]) for st in f.statuses
                ]
        return out
    start, upd = resp.responses[0].start_workflow, resp.responses[1].update_workflow
    return {
        "code": "OK",
        "start": {
            "same run": start.run_id == upd.update_ref.workflow_execution.run_id,
            "started": start.started,
            "status": start.status,
            "first run is this run": start.first_execution_run_id == start.run_id,
            "link": link_info(start.link),
        },
        "update": {
            "stage": upd.stage,
            "outcome": up.outcome(upd.outcome) if upd.HasField("outcome") else None,
            "link": link_info(upd.link),
        },
    }


async def answer(r: Run, update_id: str, close: bool = False) -> dict:
    """Poll a task and accept and complete the update it carries."""
    t = await r.poll()
    info = {"events": [short(e.event_type) for e in t.history.events], "messages": len(t.messages)}
    m = t.messages[0]
    cmds = [up.protocol_cmd(f"{update_id}/accept"), up.protocol_cmd(f"{update_id}/complete")]
    if close:
        cmds.append(complete_cmd())
    info["complete"] = (await r.complete(t.task_token, cmds, messages=[up.accept_msg(m), up.response_msg(update_id)]))[
        0
    ]
    return info


async def s_update_with_start(c):
    out = {}
    r = Run(c, "uws")
    pending = asyncio.create_task(uws(r, uws_request(r, "u1")))
    await asyncio.sleep(0.5)
    out["first task"] = await answer(r, "u1")
    out["new workflow"] = await pending
    out["history"] = await r.history()
    # The same workflow, running: the default conflict policy fails the start.
    out["running, fail policy"] = await uws(r, uws_request(r, "u2"))
    # Use-existing: only the update runs.
    pending = asyncio.create_task(
        uws(r, uws_request(r, "u3", workflow_id_conflict_policy=CONFLICT.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING))
    )
    await asyncio.sleep(0.5)
    out["use existing task"] = await answer(r, "u3")
    out["running, use existing"] = await pending
    # A completed update is answered from the history, whatever the policies.
    out["completed update again"] = await uws(r, uws_request(r, "u1"))
    # Once the workflow is closed, a new run starts (reuse policy allows it).
    await r.ws.terminate_workflow_execution(
        TerminateWorkflowExecutionRequest(namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid))
    )
    out["closed, completed update again"] = await uws(r, uws_request(r, "u1"))
    pending = asyncio.create_task(uws(r, uws_request(r, "u4")))
    await asyncio.sleep(0.5)
    out["closed, new run task"] = await answer(r, "u4")
    out["closed, new run"] = await pending
    return out


async def s_update_with_start_dedupe(c):
    r = Run(c, "uwsdedupe")
    req = uws_request(r, "u1", request_id="same-request")
    pending = asyncio.create_task(uws(r, req))
    await asyncio.sleep(0.5)
    out = {"first task": await answer(r, "u1")}
    out["first"] = await pending
    # Retrying the same start request reaches the same run (and the same update).
    out["retry"] = await uws(r, uws_request(r, "u1", request_id="same-request"))
    return out


async def s_update_with_start_errors(c):
    r = Run(c, "uwserr")
    out = {}
    req = uws_request(r, "u1")
    req.operations.append(req.operations[0])
    out["three operations"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[0].CopyFrom(req.operations[1])
    out["update first"] = await uws(r, req)
    out["cron"] = await uws(r, uws_request(r, "u1", cron_schedule="* * * * *"))
    out["start delay"] = await uws(r, uws_request(r, "u1", workflow_start_delay=secs(5)))
    out["eager"] = await uws(r, uws_request(r, "u1", request_eager_execution=True))
    req = uws_request(r, "u1")
    req.operations[1].update_workflow.workflow_execution.run_id = str(uuid.uuid4())
    out["update run id"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[1].update_workflow.first_execution_run_id = str(uuid.uuid4())
    out["update first run id"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[1].update_workflow.workflow_execution.workflow_id = "other"
    out["different workflow ids"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[1].update_workflow.namespace = "elsewhere"
    out["namespace mismatch"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[1].update_workflow.request.ClearField("input")
    out["update without input"] = await uws(r, req)
    req = uws_request(r, "u1")
    req.operations[0].start_workflow.ClearField("workflow_type")
    out["start without type"] = await uws(r, req)
    out["no workflow was started"] = await describe(r)
    return out


async def s_start_response(c):
    r = Run(c, "startresp")
    resp = await r.ws.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=ns(),
            workflow_id=r.wid,
            workflow_type=WorkflowType(name="Raw"),
            task_queue=TaskQueue(name=r.tq),
            request_id="req-1",
        )
    )
    out = {
        "new": {
            "started": resp.started,
            "first run": resp.first_execution_run_id == resp.run_id,
            "link": link_info(resp.link),
        }
    }
    again = await r.ws.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=ns(),
            workflow_id=r.wid,
            workflow_type=WorkflowType(name="Raw"),
            task_queue=TaskQueue(name=r.tq),
            request_id="req-1",
        )
    )
    out["same request"] = {
        "same run": again.run_id == resp.run_id,
        "started": again.started,
        "first run": again.first_execution_run_id == resp.run_id,
        "link": link_info(again.link),
    }
    existing = await r.ws.start_workflow_execution(
        StartWorkflowExecutionRequest(
            namespace=ns(),
            workflow_id=r.wid,
            workflow_type=WorkflowType(name="Raw"),
            task_queue=TaskQueue(name=r.tq),
            request_id="req-2",
            workflow_id_conflict_policy=CONFLICT.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
        )
    )
    out["use existing"] = {
        "same run": existing.run_id == resp.run_id,
        "started": existing.started,
        "first run": existing.first_execution_run_id == resp.run_id,
        "link": link_info(existing.link),
    }
    return out


async def options(r: Run, paths: list[str], **opts) -> dict:
    try:
        resp = await r.ws.update_workflow_execution_options(
            UpdateWorkflowExecutionOptionsRequest(
                namespace=ns(),
                workflow_execution=WorkflowExecution(workflow_id=r.wid),
                workflow_execution_options=WorkflowExecutionOptions(**opts),
                update_mask=FieldMask(paths=paths),
                identity="parity",
            )
        )
    except RPCError as e:
        return {"code": e.status.name, "message": e.message.replace(r.wid, "<wid>")}
    o = resp.workflow_execution_options
    return {
        "code": "OK",
        "priority": [o.priority.priority_key, o.priority.fairness_key, o.priority.fairness_weight]
        if o.HasField("priority")
        else None,
        "override": o.HasField("versioning_override"),
    }


async def s_options(c):
    r = Run(c, "options")
    await r.start()
    out = {"set priority": await options(r, ["priority"], priority=Priority(priority_key=3, fairness_key="a"))}
    out["same again (no change)"] = await options(r, ["priority"], priority=Priority(priority_key=3, fairness_key="a"))
    out["only fairness weight"] = await options(r, ["priority.fairness_weight"], priority=Priority(fairness_weight=2.5))
    out["clear priority"] = await options(r, ["priority"])
    out["history"] = await r.history()
    d = await r.ws.describe_workflow_execution(
        DescribeWorkflowExecutionRequest(namespace=ns(), execution=WorkflowExecution(workflow_id=r.wid))
    )
    out["describe priority"] = d.workflow_execution_info.HasField("priority")
    # A running task: the event waits for the task like a signal (and needs no new task).
    t = await r.poll()
    out["while task runs"] = await options(r, ["priority.priority_key"], priority=Priority(priority_key=1))
    out["history while task runs"] = await r.history()
    await r.complete(t.task_token)
    out["history after task"] = await r.history()
    out["bad path"] = await options(r, ["nope"])
    out["bad nested path"] = await options(r, ["priority.nope"])
    out["camel case path"] = await options(r, ["priorityKey"])
    out["negative priority"] = await options(r, ["priority"], priority=Priority(priority_key=-1))
    out["long fairness key"] = await options(r, ["priority"], priority=Priority(fairness_key="k" * 65))
    out["partial override"] = await options(r, ["versioning_override.pinned_version"])
    out["half override"] = await options(r, ["versioning_override.behavior"])
    out["time skipping"] = await options(
        r, ["time_skipping_config"], time_skipping_config=TimeSkippingConfig(enabled=True)
    )
    try:
        await r.ws.update_workflow_execution_options(
            UpdateWorkflowExecutionOptionsRequest(
                namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid)
            )
        )
        out["no mask"] = "OK"
    except RPCError as e:
        out["no mask"] = e.status.name, e.message
    try:
        await r.ws.update_workflow_execution_options(
            UpdateWorkflowExecutionOptionsRequest(
                namespace=ns(),
                workflow_execution=WorkflowExecution(workflow_id=r.wid),
                update_mask=FieldMask(paths=["priority"]),
            )
        )
        out["no options"] = "OK"
    except RPCError as e:
        out["no options"] = e.status.name, e.message
    await r.ws.terminate_workflow_execution(
        TerminateWorkflowExecutionRequest(namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid))
    )
    out["closed workflow"] = await options(r, ["priority"], priority=Priority(priority_key=2))
    out["unknown workflow"] = await options(Run(c, "nobody"), ["priority"])
    return out


async def s_pause(c):
    r = Run(c, "pause")
    await r.start()

    async def go(fn, req) -> list:
        try:
            await fn(req)
            return ["OK"]
        except RPCError as e:
            return [e.status.name, e.message.replace(ns(), "<ns>")]

    out = {
        "pause": await go(
            r.ws.pause_workflow_execution, PauseWorkflowExecutionRequest(namespace=ns(), workflow_id=r.wid)
        )
    }
    out["unpause running"] = await go(
        r.ws.unpause_workflow_execution, UnpauseWorkflowExecutionRequest(namespace=ns(), workflow_id=r.wid)
    )
    out["unpause long reason"] = await go(
        r.ws.unpause_workflow_execution,
        UnpauseWorkflowExecutionRequest(namespace=ns(), workflow_id=r.wid, reason="r" * 1001),
    )
    out["unpause unknown"] = await go(
        r.ws.unpause_workflow_execution, UnpauseWorkflowExecutionRequest(namespace=ns(), workflow_id="nobody")
    )
    await r.ws.terminate_workflow_execution(
        TerminateWorkflowExecutionRequest(namespace=ns(), workflow_execution=WorkflowExecution(workflow_id=r.wid))
    )
    out["unpause closed"] = await go(
        r.ws.unpause_workflow_execution, UnpauseWorkflowExecutionRequest(namespace=ns(), workflow_id=r.wid)
    )
    return out


async def reset(r: Run, finish: int, request_id: str | None = None, run_id: str = "", **kw) -> dict:
    try:
        resp = await r.ws.reset_workflow_execution(
            ResetWorkflowExecutionRequest(
                namespace=ns(),
                workflow_execution=WorkflowExecution(workflow_id=r.wid, run_id=run_id),
                reason="parity reset",
                workflow_task_finish_event_id=finish,
                request_id=str(uuid.uuid4()) if request_id is None else request_id,
                identity="parity",
                **kw,
            )
        )
    except RPCError as e:
        return {"code": e.status.name, "message": e.message}
    return {"code": "OK", "run_id": resp.run_id}


def event_view(e) -> str:
    """An event's type plus the details a reset sets."""
    name = short(e.event_type)
    if e.HasField("workflow_task_failed_event_attributes"):
        a = e.workflow_task_failed_event_attributes
        name += f" {WorkflowTaskFailedCause.Name(a.cause)} {a.identity} {a.failure.message!r}"
        name += " reset-info" if a.failure.HasField("reset_workflow_failure_info") else ""
    if e.HasField("workflow_execution_terminated_event_attributes"):
        a = e.workflow_execution_terminated_event_attributes
        name += f" {a.reason!r} {a.identity}"
    if e.HasField("workflow_task_scheduled_event_attributes"):
        name += f" attempt={e.workflow_task_scheduled_event_attributes.attempt}"
    if e.HasField("workflow_task_started_event_attributes"):
        name += f" {e.workflow_task_started_event_attributes.identity}"
    if e.HasField("workflow_execution_update_admitted_event_attributes"):
        a = e.workflow_execution_update_admitted_event_attributes
        name += f" {a.request.meta.update_id} {UpdateAdmittedEventOrigin.Name(a.origin)}"
    return name


async def run_history(r: Run, run_id: str) -> list[str]:
    h = await r.ws.get_workflow_execution_history(
        GetWorkflowExecutionHistoryRequest(
            namespace=ns(), execution=WorkflowExecution(workflow_id=r.wid, run_id=run_id)
        )
    )
    return [event_view(e) for e in h.history.events]


async def two_task_run(c, name: str) -> tuple[Run, str]:
    """Task 1 starts a timer; a signal; task 2 schedules an activity. Returns the run and its id."""
    r = Run(c, name)
    first = await start(r)
    t = await r.poll()
    await r.complete(t.task_token, [timer_cmd("t1")])
    await r.signal()
    t = await r.poll()
    await r.complete(t.task_token, [activity_cmd("a1", f"{r.tq}-acts")])
    await r.signal()
    return r, first


async def s_reset(c):
    out = {}
    r, base = await two_task_run(c, "reset")
    res = await reset(r, 4)
    out["reset to first task"] = {"code": res["code"], "new run": res.get("run_id") not in (None, base)}
    out["base run history"] = await run_history(r, base)
    out["base run"] = await describe(r, base)
    new = res["run_id"]
    out["reset run history"] = await run_history(r, new)
    out["current run is the reset run"] = (await describe(r))["code"] == "OK" and (
        await r.ws.describe_workflow_execution(
            DescribeWorkflowExecutionRequest(namespace=ns(), execution=WorkflowExecution(workflow_id=r.wid))
        )
    ).workflow_execution_info.execution.run_id == new
    t = await r.poll()
    out["reset run task"] = Run.task(t)
    out["complete"] = (await r.complete(t.task_token, [complete_cmd()]))[0]
    out["reset run final"] = await run_history(r, new)
    # Reset to a task that never started (the started event is added).
    out["reset to scheduled task"] = (await reset(r, 3))["code"]
    out["history"] = await r.history()
    return out


async def s_reset_options(c):
    out = {}
    r, base = await two_task_run(c, "resetopts")
    res = await reset(r, 4, reset_reapply_exclude_types=[RESET_EXCLUDE.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL])
    out["exclude signals"] = await run_history(r, res["run_id"])
    res = await reset(r, 4, reset_reapply_type=ResetReapplyType.RESET_REAPPLY_TYPE_NONE)
    out["reapply none"] = await run_history(r, res["run_id"])
    res = await reset(
        r,
        4,
        post_reset_operations=[
            PostResetOperation(
                update_workflow_options=PostResetOperation.UpdateWorkflowOptions(
                    workflow_execution_options=WorkflowExecutionOptions(priority=Priority(priority_key=4)),
                    update_mask=FieldMask(paths=["priority"]),
                )
            )
        ],
    )
    out["post reset priority"] = await run_history(r, res["run_id"])
    # The same request id again gives the same run.
    first = await reset(r, 4, request_id="reset-req")
    again = await reset(r, 4, request_id="reset-req")
    out["same request id"] = first["run_id"] == again["run_id"]
    # Reset an older run while a newer one runs.
    out["reset older run"] = (await reset(r, 4, run_id=base))["code"]
    out["runs"] = await r.history()
    return out


async def s_reset_closed(c):
    r = Run(c, "resetclosed")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [complete_cmd()])
    res = await reset(r, 4)
    out = {"reset completed workflow": res["code"]}
    out["reset run history"] = await r.history()
    t = await r.poll()
    out["task"] = Run.task(t)
    return out


async def s_reset_update(c):
    r = Run(c, "resetupd")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token)
    u = asyncio.create_task(r_update(r, "u1"))
    await asyncio.sleep(0.5)
    t = await r.poll()
    m = t.messages[0]
    await r.complete(
        t.task_token,
        [up.protocol_cmd("u1/accept"), up.protocol_cmd("u1/complete")],
        messages=[up.accept_msg(m), up.response_msg("u1")],
    )
    out = {"update": (await u)["code"]}
    res = await reset(r, 4)
    out["reset run history"] = await r.history()
    out["reset history detail"] = await run_history(r, res["run_id"])
    t = await r.poll()
    out["task"] = Run.task(t)
    out["task messages"] = len(t.messages)
    # The worker finds the update in the admitted event and answers it.
    admitted = [
        e.event_id for e in t.history.events if e.HasField("workflow_execution_update_admitted_event_attributes")
    ]
    acc = Message(
        id="u1/accept",
        protocol_instance_id="u1",
        body=up.packed(up.Acceptance(accepted_request_message_id="", accepted_request_sequencing_event_id=admitted[0])),
    )
    out["answer re-admitted update"] = (
        await r.complete(
            t.task_token,
            [up.protocol_cmd("u1/accept"), up.protocol_cmd("u1/complete")],
            messages=[acc, up.response_msg("u1", b"again")],
        )
    )[0]
    out["history after bad acceptance"] = await run_history(r, res["run_id"])
    # The retry carries the update again; a proper acceptance completes it.
    t = await r.poll()
    out["retry task"] = Run.task(t)
    acc.body.CopyFrom(
        up.packed(
            up.Acceptance(accepted_request_message_id="u1/request", accepted_request_sequencing_event_id=admitted[0])
        )
    )
    out["answer on retry"] = (
        await r.complete(
            t.task_token,
            [up.protocol_cmd("u1/accept"), up.protocol_cmd("u1/complete")],
            messages=[acc, up.response_msg("u1", b"again")],
        )
    )[0]
    out["final history"] = await run_history(r, res["run_id"])
    out["poll update"] = await up.Run.poll_update(r, "u1")
    return out


async def r_update(r: Run, update_id: str) -> dict:
    try:
        await r.ws.update_workflow_execution(up.Run.update_request(r, update_id), timeout=timedelta(seconds=30))
        return {"code": "OK"}
    except RPCError as e:
        return {"code": e.status.name}


async def s_reset_activity(c):
    r = Run(c, "resetact")
    await r.start()
    t = await r.poll()
    await r.complete(t.task_token, [activity_cmd("a1", r.tq)])
    await r.signal()
    t = await r.poll()
    await r.complete(t.task_token)
    res = await reset(r, 9)
    out = {"reset": res["code"], "history": await run_history(r, res.get("run_id", ""))}
    a = await r.ws.poll_activity_task_queue(
        PollActivityTaskQueueRequest(namespace=ns(), task_queue=TaskQueue(name=r.tq), identity="parity"),
        timeout=timedelta(seconds=10),
    )
    out["activity redispatched"] = {"id": a.activity_id, "attempt": a.attempt}
    return out


async def s_reset_errors(c):
    r, _ = await two_task_run(c, "reseterr")
    out = {"no request id": await reset(r, 4, request_id="")}
    out["long request id"] = await reset(r, 4, request_id="x" * 1001)
    out["finish id 1"] = await reset(r, 1)
    out["finish id past end"] = await reset(r, 1000)
    out["not a task event"] = await reset(r, 6)
    out["bad reapply type"] = await reset(r, 4, reset_reapply_type=9)
    out["unknown workflow"] = await reset(Run(c, "nobody"), 4)
    for v in out.values():  # ids and error hashes differ between servers
        if "message" in v:
            v["message"] = re.sub(r"wft-nobody-[0-9a-f]+", "<wid>", v["message"].replace(r.wid, "<wid>"))
            v["message"] = re.sub(r"retry \([0-9a-f]+\)", "retry (<hash>)", v["message"])
    return out


SCENARIOS = [
    s_paging,
    s_transient_last_page,
    s_long_poll,
    s_children,
    s_errors,
    s_delete,
    s_update_with_start,
    s_update_with_start_dedupe,
    s_update_with_start_errors,
    s_start_response,
    s_options,
    s_pause,
    s_reset,
    s_reset_options,
    s_reset_closed,
    s_reset_update,
    s_reset_activity,
    s_reset_errors,
]


async def main(addr: str) -> None:
    c = await Client.connect(addr)
    wp.NS = up.NS = await ready_namespace(c)
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
