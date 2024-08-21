import asyncio
import logging
import typing as t
from collections import defaultdict
from uuid import UUID

import logfire
from expression import Nothing, Option, Some
from pydantic import BaseModel, Field
from shared.common.models.luna_exception import LunaException
from shared.common.models.luna_exception_category import LunaExceptionCategory
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent, WorkflowTimeoutEvent
from shared.services.workflow.models.workflow_event_base import (
  WorkflowEventBase,
)
from shared.services.workflow.models.workflow_events_union import WorkflowSpecialEventsUnion
from shared.services.workflow.models.workflow_retry_event import WorkflowRetryEvent

logger = logging.getLogger()


_TStepEvent = t.TypeVar("_TStepEvent", bound=t.Tuple[WorkflowEventBase, ...])


class StepConfig(BaseModel):
  trigger_event_definitions: t.Tuple[t.Type[WorkflowEventBase], ...]
  step_timeout_seconds: float = 180
  max_retry_attempts: int = Field(default=0, ge=0, le=3)
  """The maximum number of retries for the step.  Should be <=3."""
  workflow_name: str = Field(default=None)
  step_name: str = Field(default=None)
  is_subtask: bool = Field(default=False)


class RunQueues(t.NamedTuple, t.Generic[_TStepEvent]):
  inbox: asyncio.Queue[WorkflowEventBase]
  """The inbox for the step"""
  multi_trigger_waiting_buffer: t.List[WorkflowEventBase]
  """The buffer for events when there are multiple trigger event required for the step to run.  It stores data as dict of event_cls -> event.  When all the events are received, the buffer is moved to the ready buffer."""
  ready_to_execute: t.List[_TStepEvent]

  process_run_lock: asyncio.Lock = asyncio.Lock()


class WorkflowStep(t.Generic[_TStepEvent]):
  """Encapsulates a step in a workflow.  An internal class used by the workflow framework."""

  def __init__(self, config: StepConfig, fn: t.Callable[[t.Self, _TStepEvent], t.Awaitable[t.Tuple[WorkflowEventBase, ...]]]):
    self.config: StepConfig = config
    self._fn: t.Callable[[t.Self, _TStepEvent], t.Awaitable[t.Tuple[WorkflowEventBase, ...]]] = fn
    self._run_queues: t.Dict[UUID, RunQueues[_TStepEvent]] = {}
    self._step_timeout_seconds: float = config.step_timeout_seconds

  async def _process_inbox_item(self, event: WorkflowEventBase, run_uuid: UUID) -> None:
    if len(self.config.trigger_event_definitions) == 1 and isinstance(event, self.config.trigger_event_definitions[0]):
      self._run_queues[run_uuid].ready_to_execute.append(t.cast(_TStepEvent, (event,)))
    else:
      self._run_queues[run_uuid].multi_trigger_waiting_buffer.append(event)

  async def _process_buffer(self, run_uuid: UUID) -> None:
    """
    Process the `multi trigger waiting buffer` and move the events to the `ready buffer`
    if all the required event types are received, regardless of order.
    """
    buffer = self._run_queues[run_uuid].multi_trigger_waiting_buffer
    required_types = self.config.trigger_event_definitions

    # Group events by type
    events_by_type: t.Dict[t.Type[WorkflowEventBase], t.List[WorkflowEventBase]] = defaultdict(list)
    for event in buffer:
      events_by_type[type(event)].append(event)

    # Check if all required types are present
    if all(req_type in events_by_type for req_type in required_types):
      # Order events according to required types
      ordered_events: t.List[WorkflowEventBase] = []
      for req_type in required_types:
        if events_by_type[req_type]:
          ordered_events.append(events_by_type[req_type].pop(0))

      # Verify the length matches
      if len(ordered_events) == len(required_types):
        self._run_queues[run_uuid].ready_to_execute.append(t.cast(_TStepEvent, tuple(ordered_events)))

        # Remove used events from the buffer
        # this clears the buffer completly rather than removing the used events.  A design choice for now.  We can also remove just used events with event_uuid, however, this means that we need to understand why there are multiple events in the multi_trigger_waiting_buffer.  e.g.: Is it a bad workflow subclass implementation or was that usecase actually required?
        self._run_queues[run_uuid].multi_trigger_waiting_buffer.clear()

  async def process_inbox(self, run_uuid: UUID) -> bool:
    async with self._run_queues[run_uuid].process_run_lock:
      try:
        event = self._run_queues[run_uuid].inbox.get_nowait()
        await self._process_inbox_item(event, run_uuid)
        await self._process_buffer(run_uuid)
        return True
      except asyncio.QueueEmpty:
        # nothing to process
        return False

  async def put_event(self, event: WorkflowEventBase, run_uuid: UUID) -> None:
    self.init_run_queues(run_uuid)
    await self._run_queues[run_uuid].inbox.put(event)

  async def execute_step(self, workflow_self: t.Any, run_uuid: UUID) -> Option[t.Tuple[WorkflowEventBase, ...] | WorkflowSpecialEventsUnion]:
    """Process the event queue from the `ready buffer` and execute the step function for that event."""
    async with self._run_queues[run_uuid].process_run_lock:
      try:
        events = self._run_queues[run_uuid].ready_to_execute.pop(0)
      except IndexError:
        return Nothing
    return Some(await self._execute(workflow_self, events, run_uuid))

  async def _execute(self, workflow_self: t.Any, events: _TStepEvent, run_uuid: UUID) -> t.Tuple[WorkflowEventBase, ...] | WorkflowSpecialEventsUnion:
    with logfire.span(
      f"Workflow:{workflow_self.__class__.__name__}::step:{self.config.step_name}",
      step_fn=self.config.step_name,
      run_uuid=run_uuid,
      trigger_events=[(event.event_uuid, event.__class__.__name__, event.model_dump(exclude={"event_uuid"})) for event in events],
    ):
      error_events: WorkflowErrorEvent
      try:
        async with asyncio.timeout(self._step_timeout_seconds):
          result = await self._fn(workflow_self, events)
          if isinstance(result, WorkflowErrorEvent):
            return self.process_result_for_retry(events, result, run_uuid)
          return result
      except asyncio.TimeoutError as cause:
        error_events = WorkflowTimeoutEvent(
          step_name=Some(self.config.step_name),
          errored_events=events,
          run_uuid=run_uuid,
          workflow_name=self.config.workflow_name,
          exception=LunaException(
            LunaExceptionCategory.store_workflow_timeout,
            description="The workflow step timed out",
            details={"step_fn": self.config.step_name, "events": events, "step_config": self.config},
            cause=cause,
          ),
        )
      except Exception as cause:
        error_events = WorkflowErrorEvent(
          step_name=Some(self.config.step_name),
          errored_events=events,
          run_uuid=run_uuid,
          workflow_name=self.config.workflow_name,
          exception=LunaException(
            LunaExceptionCategory.store_workflow_error,
            description="The workflow step raised an exception",
            cause=cause,
            details={"step_fn": self.config.step_name, "events": events, "step_config": self.config},
          ),
        )

      return self.process_result_for_retry(events, error_events, run_uuid)

  def process_result_for_retry(self, events: _TStepEvent, error_event: WorkflowErrorEvent, run_uuid: UUID) -> WorkflowSpecialEventsUnion:
    if self.config.max_retry_attempts > 0:
      attempt_number = max(event.attempt_number for event in events) + 1

      if attempt_number > self.config.max_retry_attempts:
        error_event.attempt_number = attempt_number
        return error_event

      retry_events: t.Tuple[WorkflowEventBase, ...] = tuple()

      for event in events:
        # make a copy of the event for retry and update attempts and prior_attempt_error
        new_retry_event = event.model_copy()
        new_retry_event.model_config["frozen"] = False
        new_retry_event.attempt_number = attempt_number
        new_retry_event.prior_attempt_error = Some(error_event.to_exception())
        new_retry_event.model_config["frozen"] = True
        retry_events = retry_events + (new_retry_event,)

      return WorkflowRetryEvent(
        current_step=Some(self.config.step_name),
        run_uuid=run_uuid,
        workflow_error_event=error_event,
        retry_events=retry_events,
        attempt_number=attempt_number,
      )

    return error_event

  def init_run_queues(self, run_uuid: UUID) -> None:
    """Initialize the run queues for the step if they don't exist."""
    if run_uuid not in self._run_queues:
      self._run_queues[run_uuid] = RunQueues(inbox=asyncio.Queue(), multi_trigger_waiting_buffer=[], ready_to_execute=[])

  def del_run_queues(self, run_uuid: UUID) -> None:
    """Delete the run queues for the step if they exist."""
    self._run_queues.pop(run_uuid, None)
