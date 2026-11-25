"""End-to-end tests: the official Temporal Python SDK against rustify-temporal.

Usage:  uv run --project e2e python e2e/run.py [path/to/rustify-temporal]
        uv run --project e2e python e2e/run.py --addr host:port
Starts the server on a free port with a fresh database, runs every test, restarts the server
once to check nothing is lost, and exits non-zero if anything failed. With --addr it runs
against an already-running server instead and skips the restart check.
"""

import asyncio
import os
import socket
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from datetime import timedelta
from pathlib import Path

from temporalio.api.enums.v1 import EventType
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowUpdateFailedError,
    WorkflowUpdateStage,
)
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError as TemporalTimeoutError
from temporalio.service import RPCError
from temporalio.worker import Worker

sys.path.insert(0, str(Path(__file__).parent))
import workflows as w  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
TQ = "e2e-queue"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, binary: str, port: int, db: str, log: Path):
        self.binary, self.port, self.db, self.log = binary, port, db, log
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        f = open(self.log, "a")
        env = dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "info"))
        self.proc = subprocess.Popen(
            [self.binary, "--port", str(self.port), "--db", self.db], stdout=f, stderr=f, env=env
        )
        deadline = time.time() + 15
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def stop(self) -> None:
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            self.proc = None


def wid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def t_hello(c: Client):
    r = await c.execute_workflow(w.HelloWorkflow.run, "Temporal", id=wid("hello"), task_queue=TQ)
    assert r == "Hello, Temporal!", r


async def t_activity_retry(c: Client):
    r = await c.execute_workflow(w.RetryWorkflow.run, "k1", id=wid("retry"), task_queue=TQ)
    assert r == "ok after 3 attempts", r


async def t_activity_failure(c: Client):
    try:
        await c.execute_workflow(w.FailingActivityWorkflow.run, id=wid("actfail"), task_queue=TQ)
        raise AssertionError("expected failure")
    except WorkflowFailureError as e:
        assert isinstance(e.cause, ActivityError), repr(e.cause)
        assert isinstance(e.cause.cause, ApplicationError) and e.cause.cause.type == "BadThing", repr(e.cause.cause)


async def t_signal_query(c: Client):
    h = await c.start_workflow(w.SignalQueryWorkflow.run, id=wid("sigq"), task_queue=TQ)
    await h.signal(w.SignalQueryWorkflow.add, "a")
    await h.signal(w.SignalQueryWorkflow.add, "b")
    for _ in range(50):
        if await h.query(w.SignalQueryWorkflow.count) == 2:
            break
        await asyncio.sleep(0.1)
    assert await h.query(w.SignalQueryWorkflow.count) == 2
    await h.signal(w.SignalQueryWorkflow.finish)
    assert await h.result() == ["a", "b"]
    # Queries still work after the workflow closed.
    assert await h.query(w.SignalQueryWorkflow.count) == 2


async def t_signal_with_start(c: Client):
    i = wid("sws")
    h = await c.start_workflow(
        w.SignalQueryWorkflow.run, id=i, task_queue=TQ, start_signal="add", start_signal_args=["first"]
    )
    await h.signal(w.SignalQueryWorkflow.finish)
    assert await h.result() == ["first"]


async def t_timer(c: Client):
    t0 = time.time()
    r = await c.execute_workflow(w.TimerWorkflow.run, 1.5, id=wid("timer"), task_queue=TQ)
    took = time.time() - t0
    assert r.startswith("slept 1."), r
    assert 1.4 <= took < 6, took


async def t_child(c: Client):
    assert await c.execute_workflow(w.ParentWorkflow.run, 5, id=wid("parent"), task_queue=TQ) == 20


async def t_continue_as_new(c: Client):
    h = await c.start_workflow(w.LoopWorkflow.run, 0, id=wid("loop"), task_queue=TQ)
    assert await h.result() == 3
    d = await c.get_workflow_handle(h.id).describe()
    assert d.status == WorkflowExecutionStatus.COMPLETED and d.run_id != h.result_run_id


async def t_cancel(c: Client):
    h = await c.start_workflow(w.CancellableWorkflow.run, id=wid("cancel"), task_queue=TQ)
    await asyncio.sleep(0.5)
    await h.cancel()
    assert await h.result() == "cleaned up after cancel"


async def t_cancel_activity(c: Client):
    r = await c.execute_workflow(w.CancelActivityWorkflow.run, id=wid("cancelact"), task_queue=TQ)
    assert r == "CancelledError", r


async def t_terminate(c: Client):
    h = await c.start_workflow(w.CancellableWorkflow.run, id=wid("term"), task_queue=TQ)
    await asyncio.sleep(0.3)
    await h.terminate("test")
    d = await h.describe()
    assert d.status == WorkflowExecutionStatus.TERMINATED, d.status
    try:
        await h.result()
        raise AssertionError("expected failure")
    except WorkflowFailureError:
        pass


async def t_heartbeat(c: Client):
    assert await c.execute_workflow(w.HeartbeatWorkflow.run, id=wid("hb"), task_queue=TQ) == 20


async def t_activity_timeout_retry(c: Client):
    r = await c.execute_workflow(w.TimeoutRetryWorkflow.run, id=wid("tmo"), task_queue=TQ)
    assert r == "attempt 2", r


async def t_signal_external(c: Client):
    target = await c.start_workflow(w.SignalQueryWorkflow.run, id=wid("target"), task_queue=TQ)
    assert await c.execute_workflow(w.SignalSenderWorkflow.run, target.id, id=wid("sender"), task_queue=TQ) == "sent"
    assert await target.result() == ["from-another-workflow"]


async def t_workflow_failure(c: Client):
    try:
        await c.execute_workflow(w.FailingWorkflow.run, id=wid("wffail"), task_queue=TQ)
        raise AssertionError("expected failure")
    except WorkflowFailureError as e:
        assert isinstance(e.cause, ApplicationError) and "gave up" in e.cause.message, repr(e.cause)


async def t_workflow_retry(c: Client):
    from temporalio.common import RetryPolicy

    r = await c.execute_workflow(
        w.RetryingWorkflow.run,
        id=wid("wfretry"),
        task_queue=TQ,
        retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
    )
    assert r == 3, r


async def t_already_started(c: Client):
    from temporalio.exceptions import WorkflowAlreadyStartedError

    i = wid("dup")
    h = await c.start_workflow(w.SignalQueryWorkflow.run, id=i, task_queue=TQ)
    try:
        await c.start_workflow(w.SignalQueryWorkflow.run, id=i, task_queue=TQ)
        raise AssertionError("expected WorkflowAlreadyStartedError")
    except WorkflowAlreadyStartedError:
        pass
    await h.signal(w.SignalQueryWorkflow.finish)
    await h.result()


async def t_many_activities(c: Client):
    assert await c.execute_workflow(w.ManyActivitiesWorkflow.run, 25, id=wid("many"), task_queue=TQ) == 25


async def t_local_activity(c: Client):
    assert (
        await c.execute_workflow(w.LocalActivityWorkflow.run, "local", id=wid("la"), task_queue=TQ) == "Hello, local!"
    )


async def t_describe_list(c: Client):
    i = wid("listme")
    await c.execute_workflow(w.HelloWorkflow.run, "x", id=i, task_queue=TQ)
    d = await c.get_workflow_handle(i).describe()
    assert d.status == WorkflowExecutionStatus.COMPLETED and d.workflow_type == "HelloWorkflow"
    found = [x async for x in c.list_workflows(f"WorkflowId = '{i}'")]
    assert len(found) == 1 and found[0].id == i, found
    n = await c.count_workflows("WorkflowType = 'HelloWorkflow' AND ExecutionStatus = 'Completed'")
    assert n.count >= 1, n
    hist = await c.get_workflow_handle(i).fetch_history()
    assert len(hist.events) >= 5


async def t_unknown_workflow(c: Client):
    try:
        await c.get_workflow_handle("does-not-exist").describe()
        raise AssertionError("expected not found")
    except RPCError as e:
        assert "not found" in e.message.lower(), e.message


async def t_execution_timeout(c: Client):
    try:
        await c.execute_workflow(
            w.CancellableWorkflow.run, id=wid("wftimeout"), task_queue=TQ, execution_timeout=timedelta(seconds=1)
        )
        raise AssertionError("expected timeout")
    except WorkflowFailureError as e:
        assert isinstance(e.cause, TemporalTimeoutError), repr(e.cause)


async def t_update(c: Client):
    h = await c.start_workflow(w.UpdateWorkflow.run, id=wid("update"), task_queue=TQ)
    assert await h.execute_update(w.UpdateWorkflow.add, "a") == 1
    # Rejected by the validator: nothing is written to the history.
    try:
        await h.execute_update(w.UpdateWorkflow.add, "")
        raise AssertionError("expected rejection")
    except WorkflowUpdateFailedError as e:
        assert "empty item" in str(e.cause), repr(e.cause)
    try:
        await h.execute_update(w.UpdateWorkflow.fail)
        raise AssertionError("expected failure")
    except WorkflowUpdateFailedError as e:
        assert "on purpose" in str(e.cause), repr(e.cause)
    assert await h.execute_update(w.UpdateWorkflow.greet, "update") == "Hello, update!"
    # Wait for acceptance only, then for the result through the handle.
    u = await h.start_update(w.UpdateWorkflow.add, "b", wait_for_stage=WorkflowUpdateStage.ACCEPTED, id="add-b")
    assert await u.result() == 3
    # The same update id again gives the same result without running it twice.
    assert await h.execute_update(w.UpdateWorkflow.add, "b", id="add-b") == 3
    assert await h.get_update_handle("add-b").result() == 3
    await h.signal(w.UpdateWorkflow.finish)
    assert await h.result() == ["a", "Hello, update!", "b"]
    kinds = [e.event_type for e in (await h.fetch_history()).events]
    assert kinds.count(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED) == 4, kinds
    assert kinds.count(EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED) == 4, kinds


TESTS = [
    t_hello,
    t_activity_retry,
    t_activity_failure,
    t_signal_query,
    t_signal_with_start,
    t_timer,
    t_child,
    t_continue_as_new,
    t_cancel,
    t_cancel_activity,
    t_terminate,
    t_heartbeat,
    t_activity_timeout_retry,
    t_signal_external,
    t_workflow_failure,
    t_workflow_retry,
    t_already_started,
    t_many_activities,
    t_local_activity,
    t_describe_list,
    t_unknown_workflow,
    t_execution_timeout,
    t_update,
]


def worker(c: Client) -> Worker:
    return Worker(c, task_queue=TQ, workflows=w.ALL_WORKFLOWS, activities=w.ALL_ACTIVITIES)


async def run_tests(client: Client) -> int:
    failures = 0
    async with worker(client):
        for t in TESTS:
            t0 = time.time()
            try:
                await asyncio.wait_for(t(client), timeout=60)
                print(f"PASS  {t.__name__[2:]:<24} {time.time() - t0:5.1f}s", flush=True)
            except Exception:
                failures += 1
                print(f"FAIL  {t.__name__[2:]:<24} {time.time() - t0:5.1f}s", flush=True)
                traceback.print_exc()
    return failures


async def main_external(addr: str) -> int:
    client = await Client.connect(addr, namespace="default")
    failures = await run_tests(client)
    print(f"\n{len(TESTS) - failures}/{len(TESTS)} passed against {addr} (restart check skipped)")
    return 1 if failures else 0


async def main() -> int:
    if len(sys.argv) > 2 and sys.argv[1] == "--addr":
        return await main_external(sys.argv[2])
    binary = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "target" / "debug" / "rustify-temporal")
    tmp = Path(tempfile.mkdtemp(prefix="rustify-temporal-e2e-"))
    port = free_port()
    log = tmp / "server.log"
    server = Server(binary, port, str(tmp / "temporal.db"), log)
    server.start()
    failures = 0
    try:
        client = await Client.connect(f"127.0.0.1:{port}", namespace="default")
        failures += await run_tests(client)

        # Durability: a workflow waiting on a signal survives a server restart.
        t0 = time.time()
        try:
            i = wid("durable")
            h = await client.start_workflow(w.SignalQueryWorkflow.run, id=i, task_queue=TQ)
            async with worker(client):
                await h.signal(w.SignalQueryWorkflow.add, "before-restart")
                for _ in range(50):
                    if await h.query(w.SignalQueryWorkflow.count) == 1:
                        break
                    await asyncio.sleep(0.1)
            server.stop()
            server.start()
            client = await Client.connect(f"127.0.0.1:{port}", namespace="default")
            h = client.get_workflow_handle(i)
            async with worker(client):
                await h.signal(w.SignalQueryWorkflow.add, "after-restart")
                await h.signal(w.SignalQueryWorkflow.finish)
                r = await asyncio.wait_for(h.result(), timeout=30)
            assert r == ["before-restart", "after-restart"], r
            print(f"PASS  {'survives_restart':<24} {time.time() - t0:5.1f}s")
        except Exception:
            failures += 1
            print(f"FAIL  {'survives_restart':<24} {time.time() - t0:5.1f}s")
            traceback.print_exc()
    finally:
        server.stop()
    total = len(TESTS) + 1
    print(f"\n{total - failures}/{total} passed (server log: {log})")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
