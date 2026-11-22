"""Workflows and activities used by the end-to-end tests (official Temporal Python SDK)."""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

ATTEMPTS: dict[str, int] = {}


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@activity.defn
async def flaky(key: str) -> str:
    ATTEMPTS[key] = ATTEMPTS.get(key, 0) + 1
    if activity.info().attempt < 3:
        raise RuntimeError(f"boom on attempt {activity.info().attempt}")
    return f"ok after {activity.info().attempt} attempts"


@activity.defn
async def always_fails() -> str:
    raise ApplicationError("nope", type="BadThing", non_retryable=True)


@activity.defn
async def heartbeat_then_finish(n: int) -> int:
    start = activity.info().heartbeat_details[0] if activity.info().heartbeat_details else 0
    for i in range(start, n):
        activity.heartbeat(i)
        await asyncio.sleep(0.05)
    return n


@activity.defn
async def slow_without_heartbeat() -> str:
    if activity.info().attempt == 1:
        await asyncio.sleep(3)
    return f"attempt {activity.info().attempt}"


@activity.defn
async def wait_for_cancel() -> str:
    try:
        while True:
            activity.heartbeat()
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        # Tell the server the activity accepted the cancellation.
        raise


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(say_hello, name, start_to_close_timeout=timedelta(seconds=10))


@workflow.defn
class RetryWorkflow:
    @workflow.run
    async def run(self, key: str) -> str:
        return await workflow.execute_activity(
            flaky,
            key,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100), maximum_attempts=5),
        )


@workflow.defn
class FailingActivityWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(always_fails, start_to_close_timeout=timedelta(seconds=10))


@workflow.defn
class SignalQueryWorkflow:
    def __init__(self) -> None:
        self.items: list[str] = []
        self.done = False

    @workflow.run
    async def run(self) -> list[str]:
        await workflow.wait_condition(lambda: self.done)
        return self.items

    @workflow.signal
    def add(self, item: str) -> None:
        self.items.append(item)

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.query
    def count(self) -> int:
        return len(self.items)


@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, seconds: float) -> str:
        t0 = workflow.now()
        await asyncio.sleep(seconds)
        return f"slept {(workflow.now() - t0).total_seconds():.1f}"


@workflow.defn
class ChildWorkflow:
    @workflow.run
    async def run(self, n: int) -> int:
        return n * 2


@workflow.defn
class ParentWorkflow:
    @workflow.run
    async def run(self, n: int) -> int:
        a = await workflow.execute_child_workflow(ChildWorkflow.run, n, id=f"{workflow.info().workflow_id}-child-a")
        b = await workflow.execute_child_workflow(ChildWorkflow.run, a, id=f"{workflow.info().workflow_id}-child-b")
        return b


@workflow.defn
class LoopWorkflow:
    @workflow.run
    async def run(self, i: int) -> int:
        if i < 3:
            workflow.continue_as_new(i + 1)
        return i


@workflow.defn
class CancellableWorkflow:
    @workflow.run
    async def run(self) -> str:
        try:
            await asyncio.sleep(3600)
            return "finished"
        except asyncio.CancelledError:
            return "cleaned up after cancel"


@workflow.defn
class CancelActivityWorkflow:
    @workflow.run
    async def run(self) -> str:
        handle = workflow.start_activity(
            wait_for_cancel,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=2),
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        await asyncio.sleep(0.5)
        handle.cancel()
        try:
            await handle
            return "not cancelled"
        except Exception as e:  # ActivityError(cause=CancelledError)
            return type(e.__cause__ or e).__name__


@workflow.defn
class HeartbeatWorkflow:
    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            heartbeat_then_finish,
            20,
            start_to_close_timeout=timedelta(seconds=10),
            heartbeat_timeout=timedelta(seconds=2),
        )


@workflow.defn
class TimeoutRetryWorkflow:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            slow_without_heartbeat,
            start_to_close_timeout=timedelta(seconds=1),
            retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=100)),
        )


@workflow.defn
class SignalSenderWorkflow:
    @workflow.run
    async def run(self, target: str) -> str:
        h = workflow.get_external_workflow_handle(target)
        await h.signal(SignalQueryWorkflow.add, "from-another-workflow")
        await h.signal(SignalQueryWorkflow.finish)
        return "sent"


@workflow.defn
class FailingWorkflow:
    @workflow.run
    async def run(self) -> str:
        raise ApplicationError("workflow gave up", non_retryable=True)


@workflow.defn
class RetryingWorkflow:
    @workflow.run
    async def run(self) -> int:
        if workflow.info().attempt < 3:
            raise ApplicationError(f"failing attempt {workflow.info().attempt}")
        return workflow.info().attempt


@workflow.defn
class ManyActivitiesWorkflow:
    @workflow.run
    async def run(self, n: int) -> int:
        results = await asyncio.gather(
            *[
                workflow.execute_activity(say_hello, str(i), start_to_close_timeout=timedelta(seconds=10))
                for i in range(n)
            ]
        )
        return len(results)


@workflow.defn
class LocalActivityWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_local_activity(say_hello, name, start_to_close_timeout=timedelta(seconds=5))


ALL_WORKFLOWS = [
    HelloWorkflow,
    RetryWorkflow,
    FailingActivityWorkflow,
    SignalQueryWorkflow,
    TimerWorkflow,
    ChildWorkflow,
    ParentWorkflow,
    LoopWorkflow,
    CancellableWorkflow,
    CancelActivityWorkflow,
    HeartbeatWorkflow,
    TimeoutRetryWorkflow,
    SignalSenderWorkflow,
    FailingWorkflow,
    RetryingWorkflow,
    ManyActivitiesWorkflow,
    LocalActivityWorkflow,
]
ALL_ACTIVITIES = [say_hello, flaky, always_fails, heartbeat_then_finish, slow_without_heartbeat, wait_for_cancel]
