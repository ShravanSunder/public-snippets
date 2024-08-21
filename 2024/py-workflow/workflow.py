import asyncio
import logging
import random
import typing as t
from abc import ABC, abstractmethod
from time import monotonic
from types import MappingProxyType
from uuid import UUID

import logfire
from expression import Error, Nothing, Ok, Option, Result, Some
from lagom import Container
from pydantic import BaseModel
from shared.common.models.luna_exception import LunaException
from shared.common.models.luna_exception_category import LunaExceptionCategory
from shared.services.workflow.models.workflow_cancelled_event import WorkflowCancelledEvent
from shared.services.workflow.models.workflow_context_base import WorkflowContextBase
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowAlreadyRunningException, WorkflowErrorEvent, WorkflowTimeoutEvent
from shared.services.workflow.models.workflow_event_base import (
  WorkflowEventBase,
)
from shared.services.workflow.models.workflow_events_union import AllWorkflowEventsUnion, WorkflowSpecialEventsUnion
from shared.services.workflow.models.workflow_retry_event import WorkflowRetryEvent
from shared.services.workflow.models.workflow_run_result import WorkflowRunResult
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus
from shared.services.workflow.models.workflow_wait_for_Subtask_event import WorkflowWaitForSubtaskEvent
from shared.services.workflow.workflow_step import StepConfig, WorkflowStep

logger = logging.getLogger()


_TContext = t.TypeVar("_TContext", bound=WorkflowContextBase)
_TWorkflowEndEvent = t.TypeVar("_TWorkflowEndEvent", bound=WorkflowEndEventBase)
_TWorkflowStartPayload = t.TypeVar("_TWorkflowStartPayload", bound=BaseModel | None)


class WorkflowControlMixin(ABC, t.Generic[_TContext, _TWorkflowStartPayload]):
  """A base for Workflow class that defines signature for control methods of the workflow and context management."""

  def __init__(self, context: _TContext):
    self._cancelled: asyncio.Event = asyncio.Event()
    self._context_write_lock: asyncio.Lock = asyncio.Lock()
    self.__context: _TContext = context
    """DO NOT MODIFY THIS DIRECTLY.  Use the context property instead."""

  async def cancel(self):
    """Cancel the workflow execution."""
    self._cancelled.set()

  @abstractmethod
  async def start(self, payload: _TWorkflowStartPayload) -> t.Tuple[WorkflowEventBase, ...]:
    """Abstract method to be implemented by subclasses for workflow initialization."""
    raise NotImplementedError("start must be implemented by subclasses")

  @property
  def context(self) -> _TContext:
    """
    Get the current context of the workflow.

    Returns:
        _TContext: The current context.
    """
    return self.__context

  async def set_context(self, new_context: _TContext):
    """
    Set a new context for the workflow, replacing the existing context.

    Args:
        new_context (_TContext): The new context to set.
    """
    async with self._context_write_lock:
      self.__context = new_context
    return self.__context

  async def update_context(self, updater: t.Callable[[_TContext], _TContext]) -> None:
    """
    Update the context using a provided updater function.

    Args:
        updater: A function that takes the current context and returns a new context.
    """
    async with self._context_write_lock:
      new_context = updater(self.__context)
      self.__context = new_context


class Workflow(WorkflowControlMixin[_TContext, _TWorkflowStartPayload], ABC, t.Generic[_TContext, _TWorkflowStartPayload, _TWorkflowEndEvent]):
  """
    Abstract base class for workflows.

    This class provides the core functionality for defining and running workflows,
    including event routing and step execution.

    User guide:
    ```python
    # 1. Define a workflow by subclassing Workflow.   The output of one step is the input to the next step. Workflows steps must be async functions.  The last step must be an EndEvent and subclass `WorkflowEndEventBase`.

    class UseAgentToolsWorkflow(
    Workflow[UseAgentToolsNamespace.UseAgentToolWorkflowContext, UseAgentToolsNamespace.ToolRequest, UseAgentToolsNamespace.UseAgentToolEndEvent]
  ):
      async def start(
        self,
        payload: UseAgentToolsNamespace.ToolRequest,
      ) -> t.Tuple[UseAgentToolsNamespace.ToolShortlistEvent]:
        return (UseAgentToolsNamespace.ToolShortlistEvent(),)

      @step()
      async def tool_shortlist(
        self, events: t.Tuple[UseAgentToolsNamespace.ToolShortlistEvent]
      ) -> t.Tuple[UseAgentToolsNamespace.SelectToolsEvent] | WorkflowErrorEvent:
        raise NotImplementedError()

      @step()
      async def select_tools(
        self, events: t.Tuple[UseAgentToolsNamespace.SelectToolsEvent]
      ) -> t.Tuple[UseAgentToolsNamespace.InvokeToolsEvent] | WorkflowErrorEvent:
        raise NotImplementedError()


    # 2. Workflows must end with an EndEvent.  As seen above

    ```
  """

  _event_hash_to_step_fn_mapping: t.ClassVar[MappingProxyType[str, WorkflowStep[t.Any]]] = MappingProxyType({})
  _step_fn_registry: t.ClassVar[MappingProxyType[str, WorkflowStep[t.Any]]] = MappingProxyType({})

  @classmethod
  def __init_subclass__(cls, **kwargs: t.Any):
    super().__init_subclass__(**kwargs)
    cls._register_steps()

  @classmethod
  def _register_steps(cls):
    step_registry: t.Dict[str, WorkflowStep[t.Any]] = {}
    event_hash_to_step_mapping: t.Dict[str, WorkflowStep[t.Any]] = {}

    for step_fn in cls.__dict__.values():
      config: StepConfig | None = getattr(step_fn, "_StepConfig", None)
      if isinstance(config, StepConfig):
        workflow_step = WorkflowStep(
          config=config,
          fn=step_fn,
        )
        step_registry[config.step_name] = workflow_step

        for trigger_event in config.trigger_event_definitions:
          event_hash = trigger_event.cls_hash_key()
          event_hash_to_step_mapping[event_hash] = workflow_step

    cls._step_fn_registry = MappingProxyType(step_registry)
    cls._event_hash_to_step_fn_mapping = MappingProxyType(event_hash_to_step_mapping)

  def __init__(self, run_uuid: UUID, *, context: _TContext, container: Container, workflow_watchdog_seconds: float = 300):
    """
    Create a new workflow run instance

    Args:
        context (_TContext): The context for the workflow.
        run_uuid (UUID): The UUID for the workflow run.  Should be unique to each start
        workflow_watchdog_seconds (float, optional): The number of seconds to wait for event to be triggered before timing out. Defaults to 300. This is meant to signal invalid state transitions. As a default an event is expected atleast every 5 minutes.
    """
    super().__init__(context)

    self._run_uuid: UUID = run_uuid
    self.__started: bool = False
    self.__run_result: WorkflowRunResult = WorkflowRunResult(end_event=Nothing, status=WorkflowRunStatus.not_started)
    self._container: Container = container
    self._workflow_watchdog_seconds: float = workflow_watchdog_seconds

    # init run queues
    for workflow_step in self._step_fn_registry.values():
      workflow_step.init_run_queues(run_uuid)

  def __del__(self):
    for workflow_step in self._step_fn_registry.values():
      try:
        workflow_step.del_run_queues(self._run_uuid)
      except Exception as e:
        logger.error(f"Error deleting run queues for workflow step {workflow_step.config.trigger_event_definitions}: {e}")

  @property
  def run_uuid(self) -> UUID:
    return self._run_uuid

  async def send_event(self, event: WorkflowEventBase):
    """
    Send an event to all relevant steps in the workflow.

    Args:
        event (StepEvent): The event to be sent.
    """
    event_hash = event.cls_hash_key()
    if event_hash in self._event_hash_to_step_fn_mapping:
      step_fn = self._event_hash_to_step_fn_mapping[event_hash]

      if type(event) in step_fn.config.trigger_event_definitions:  # exact match, no subclass matching
        await step_fn.put_event(event, self._run_uuid)

  def _set_end_result(self, end_result: Option[WorkflowEndEventBase | WorkflowCancelledEvent | WorkflowErrorEvent], status: WorkflowRunStatus) -> None:
    self.__run_result = WorkflowRunResult(end_event=end_result, status=status)

  def get_run_result(self) -> WorkflowRunResult:
    return self.__run_result

  async def start_run(self, payload: _TWorkflowStartPayload) -> t.AsyncGenerator[AllWorkflowEventsUnion, None]:
    """
    Run the workflow, yielding state feedback and returning the final output.  You can only start one run at a time.  If you try to start a run when one is already in progress, you will get a WorkflowAlreadyRunningException.

    There are three options for how to use this method:
    1. Use as a context manager:
      ```python
      async with workflow.start_run(start_event) as run:
        async for event in run:
          print(event)
      ```

    2. Use as a generator loop:
      ```python
      run = workflow.start_run(start_event)
      async for event in run:
        print(event)
      ```

    3. Use as a coroutine and generator:
      ```python
      run = workflow.start_run(start_event)
      event = await anext(run)
      ```
    """
    with logfire.span(f"Workflow:{self.__class__.__name__}:start_run", run_uuid=self._run_uuid):
      if self.__started:
        # if the workflow is already started, return an error
        yield WorkflowErrorEvent(
          errored_events=None,
          step_name=Nothing,
          run_uuid=self._run_uuid,
          workflow_name=self.__class__.__name__,
          exception=WorkflowAlreadyRunningException(
            LunaExceptionCategory.store_workflow_error,
            description="Workflow already started",
            cause=None,
          ),
        )

        return

      try:
        # Start the workflow
        self._cancelled.clear()
        self._set_end_result(Nothing, WorkflowRunStatus.in_progress)
        self.__started = True

        start_events: t.Tuple[WorkflowEventBase, ...] = await self.start(payload)
        yield start_events
        for event in start_events:
          await self.send_event(event)

        visited_events: set[str] = set()

        # Process the workflow events until the workflow is done as a generator
        async for received_event in self._wait_for_event():
          visited_events.add(str(received_event))
          if str(received_event) in visited_events:
            logger.warning(f"Cycle detected in workflow event {received_event} was visited more than once")

          if self._cancelled.is_set():
            self._set_end_result(Some(WorkflowCancelledEvent(run_uuid=self._run_uuid)), WorkflowRunStatus.completed)
            yield WorkflowCancelledEvent(
              run_uuid=self._run_uuid,
            )
            break
          else:
            if isinstance(received_event, WorkflowCancelledEvent):
              self._set_end_result(Some(received_event), WorkflowRunStatus.completed)
              yield received_event
              break

            elif isinstance(received_event, WorkflowErrorEvent):
              if received_event.attempt_number > 0:
                received_event.exception.prepend_description(f"Workflow step failed desipte retrying {received_event.attempt_number} times")
                received_event.exception.log(logging.ERROR)
              else:
                received_event.exception.log(logging.ERROR)
              self._set_end_result(Some(received_event), WorkflowRunStatus.failed)
              yield received_event
              break

            elif isinstance(received_event, WorkflowEndEventBase):
              self._set_end_result(Some(received_event), WorkflowRunStatus.completed)
              yield received_event
              break

            elif isinstance(received_event, WorkflowRetryEvent):
              retry_events = received_event.retry_events
              yield received_event

              await self.backoff_delay(received_event)
              for retry_event in retry_events:
                await self.send_event(retry_event)
            elif isinstance(received_event, WorkflowWaitForSubtaskEvent):
              try:
                async with asyncio.timeout(received_event.timeout_seconds):
                  task_result_event = await received_event.task()
              except Exception as e:
                task_result_event = WorkflowErrorEvent(
                  exception=LunaException.from_exception(
                    e, LunaExceptionCategory.store_workflow_error, "Error in workflow", details={"execute_subtask_event": received_event}
                  ),
                  run_uuid=self._run_uuid,
                  errored_events=None,
                  step_name=Some(f"ExecuteSubtaskEvent: {received_event.task.__name__}"),
                  workflow_name=self.__class__.__name__,
                )

              if isinstance(task_result_event, WorkflowErrorEvent):
                yield task_result_event
                break
              else:
                yield task_result_event

                # send the result events to the workflow to proceed to next steps
                for event in task_result_event:
                  logger.info(f"Sending event to workflow: {event}")
                  await self.send_event(event)

            elif len(received_event) == 0:
              # if the event is empty, we need to return an error, this is an invalid state
              error = WorkflowErrorEvent(
                errored_events=None,
                step_name=Nothing,
                run_uuid=self._run_uuid,
                workflow_name=self.__class__.__name__,
                exception=LunaException(
                  LunaExceptionCategory.store_workflow_error,
                  description="No event was returned from the workflow",
                  cause=None,
                ),
              )

              self._set_end_result(Some(error), WorkflowRunStatus.failed)
              yield error
              break
            else:
              if len(received_event) > 0 and isinstance(received_event[0], WorkflowEndEventBase):
                self._set_end_result(Some(received_event[0]), WorkflowRunStatus.completed)
                yield received_event
                break

              yield received_event
              # send the result events to the workflow to proceed to next steps
              for event in received_event:
                logger.info(f"Sending event to workflow: {event}")
                await self.send_event(event)

      except asyncio.CancelledError:
        # Handle cancellation
        self._set_end_result(Some(WorkflowCancelledEvent(run_uuid=self._run_uuid)), WorkflowRunStatus.completed)
        yield WorkflowCancelledEvent(
          run_uuid=self._run_uuid,
        )
      except Exception as error:
        workflow_error = WorkflowErrorEvent(
          errored_events=None,
          step_name=Nothing,
          run_uuid=self._run_uuid,
          workflow_name=self.__class__.__name__,
          exception=LunaException.from_exception(error, LunaExceptionCategory.store_workflow_error, "Error in workflow"),
        )

        self._set_end_result(
          Some(workflow_error),
          WorkflowRunStatus.failed,
        )
        yield workflow_error

  async def backoff_delay(self, event: WorkflowRetryEvent):
    delay: float = 4.0 ** (event.attempt_number) + random.random()
    await asyncio.sleep(delay)
    logger.warning(f"Retrying workflow step {event} after {delay} seconds due to WorkflowErrorEvent")

  async def run_until_end(self, payload: _TWorkflowStartPayload) -> Result[_TWorkflowEndEvent, LunaException]:
    """Run the workflow and return the final output. Calls run_with_feedback and returns the final event."""
    current_event: AllWorkflowEventsUnion = WorkflowErrorEvent(
      run_uuid=self._run_uuid,
      workflow_name=self.__class__.__name__,
      errored_events=None,
      step_name=Nothing,
      exception=LunaException(
        LunaExceptionCategory.store_workflow_error,
        description="No event was returned from the workflow",
        cause=None,
      ),
    )

    # use start_run generator and loop until we are done
    async for event in self.start_run(payload):
      current_event = event

    if isinstance(current_event, WorkflowErrorEvent):
      return Error(current_event.to_exception())
    elif isinstance(current_event, WorkflowEndEventBase):
      return Ok(t.cast(_TWorkflowEndEvent, current_event))
    elif isinstance(current_event, WorkflowCancelledEvent):
      return Error(
        LunaException(
          LunaExceptionCategory.store_workflow_cancelled,
          description="Workflow was cancelled",
          details={"run_uuid": self._run_uuid, "current_event": current_event},
          cause=None,
        )
      )
    else:
      return Error(
        LunaException(
          LunaExceptionCategory.store_workflow_error,
          description="Workflow did not end with an EndEvent or CancelledEvent",
          details={
            "current_event": current_event,
            "run_uuid": self._run_uuid,
          },
          cause=None,
        )
      )

  async def _wait_for_event(
    self,
  ) -> t.AsyncGenerator[t.Tuple[WorkflowEventBase, ...] | WorkflowCancelledEvent | WorkflowSpecialEventsUnion, None]:
    """Wait for one or more workflow events to be processed by the steps."""
    should_wait = True
    watchdog_start_time = monotonic()

    while should_wait:
      process_results: t.List[bool] = await asyncio.gather(
        *[self._process_inbox(step_fn) for step_fn in self._step_fn_registry.values()], return_exceptions=True
      )
      for pr in process_results:
        if isinstance(pr, asyncio.CancelledError):
          self._cancelled.set()
          should_wait = False
          break
        if pr:
          watchdog_start_time = monotonic()

      execute_results: t.List[Option[t.Tuple[WorkflowEventBase, ...]]] = await asyncio.gather(
        *[self._execute_step(step_fn) for step_fn in self._step_fn_registry.values()], return_exceptions=True
      )
      for er in execute_results:
        if isinstance(er, asyncio.CancelledError):
          self._cancelled.set()
          should_wait = False
          break
        if er.is_some():
          yield er.value

      if self._cancelled.is_set():
        should_wait = False
        yield WorkflowCancelledEvent(run_uuid=self._run_uuid)
        break

      if monotonic() - watchdog_start_time > self._workflow_watchdog_seconds:
        should_wait = False
        yield WorkflowTimeoutEvent(
          run_uuid=self._run_uuid,
          step_name=Nothing,
          workflow_name=self.__class__.__name__,
          errored_events=None,
          exception=LunaException(
            LunaExceptionCategory.store_workflow_timeout,
            description=f"Workflow watchdog timed out.  There were no events for {self._workflow_watchdog_seconds} seconds",
            cause=None,
          ),
        )
        break

      # async sleep to avoid busy looping
      await asyncio.sleep(0.01)

  async def _process_inbox(self, step_fn: WorkflowStep[t.Any]) -> bool:
    if self._cancelled.is_set():
      raise asyncio.CancelledError()
    return await step_fn.process_inbox(self._run_uuid)

  async def _execute_step(self, step_fn: WorkflowStep[t.Any]) -> Option[t.Tuple[WorkflowEventBase, ...] | WorkflowSpecialEventsUnion]:
    if self._cancelled.is_set():
      raise asyncio.CancelledError()
    return await step_fn.execute_step(self, self._run_uuid)

  def workflow_error_event(self, exception: LunaException) -> WorkflowErrorEvent:
    """Used to easily create a WorkflowErrorEvent for a step.  The step_name, errored_events are enriched later with the step decorator post processes"""

    return WorkflowErrorEvent(
      errored_events=None,
      step_name=Nothing,
      workflow_name=self.__class__.__name__,
      run_uuid=self._run_uuid,
      exception=exception,
    )
