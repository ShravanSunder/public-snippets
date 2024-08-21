import typing as t

from expression import Some
from pydantic import BaseModel, Field
from shared.common.models.luna_exception import LunaException
from shared.common.models.luna_exception_category import LunaExceptionCategory
from shared.services.workflow.models.workflow_context_base import WorkflowContextBase
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase
from shared.services.workflow.models.workflow_wait_for_Subtask_event import WorkflowWaitForSubtaskEvent
from shared.services.workflow.workflow import (
  Workflow,
)
from shared.services.workflow.workflow_step_decorator import step
from uuid_utils.compat import uuid7


class MockWorkflowContext(WorkflowContextBase):
  value: str = ""


class MockStartWorkflowEvent(BaseModel):
  initial_value: str


class MockStep1Event(WorkflowEventBase):
  result: str


class MockStep2AEvent(WorkflowEventBase):
  result: str


class MockStep2BEvent(WorkflowEventBase):
  result: str


class MockStep3EventA(WorkflowEventBase):
  combined_result: str


class MockStep3EventB(WorkflowEventBase):
  combined_result: str


class MockEndEvent(WorkflowEndEventBase):
  pass


class MockWorkflow(Workflow[MockWorkflowContext, MockStartWorkflowEvent, MockEndEvent]):
  """A simple mock workflow with concurrent branching logic that eventually merges back together."""

  async def start(self, payload: MockStartWorkflowEvent) -> t.Tuple[MockStep1Event]:
    return (MockStep1Event(result=payload.initial_value),)

  @step()
  async def step_1(self, events: t.Tuple[MockStep1Event]) -> t.Tuple[MockStep2AEvent, MockStep2BEvent] | WorkflowErrorEvent:
    event = events[0]
    print(f"Step 1: Processing {event.result}")
    return (MockStep2AEvent(result=f"{event.result}_A"), MockStep2BEvent(result=f"{event.result}_B"))

  @step()
  async def step_2a(self, events: t.Tuple[MockStep2AEvent]) -> t.Tuple[MockStep3EventA] | WorkflowErrorEvent:
    event = events[0]
    print(f"Step 2A: Processing {event.result}")
    return (MockStep3EventA(combined_result=event.result),)

  @step()
  async def step_2b(self, events: t.Tuple[MockStep2BEvent]) -> t.Tuple[MockStep3EventB]:
    event = events[0]
    print(f"Step 2B: Processing {event.result}")
    return (MockStep3EventB(combined_result=event.result),)

  @step()
  async def step_3(self, events: t.Tuple[MockStep3EventA, MockStep3EventB]) -> MockEndEvent:
    print(f"Step 3: Joining results - {events[0].combined_result} and {events[1].combined_result}")
    final_result = f"{events[0].combined_result}_{events[1].combined_result}"
    await self.update_context(lambda ctx: MockWorkflowContext(value=final_result))
    return MockEndEvent(run_uuid=self._run_uuid)


class MockWorkflowWithError(Workflow[MockWorkflowContext, MockStartWorkflowEvent, MockEndEvent]):
  """A simple mock workflow with a step that fails."""

  async def start(self, payload: MockStartWorkflowEvent) -> t.Tuple[MockStep1Event]:
    return (MockStep1Event(result=payload.initial_value),)

  @step()
  async def step_2b(self, events: t.Tuple[MockStep2BEvent]) -> t.Tuple[WorkflowEventBase, ...] | WorkflowErrorEvent:
    # Create the error without the workflow and step information
    error = LunaException(LunaExceptionCategory.store_workflow_error, description="Test error in step_2b", cause=None)
    # Use create_step_error to add the workflow and step information
    return self.workflow_error_event(error)

  @step()
  async def step_1(self, events: t.Tuple[MockStep1Event]) -> t.Tuple[MockStep2AEvent, MockStep2BEvent] | WorkflowErrorEvent:
    event = events[0]
    print(f"Step 1: Processing {event.result}")
    return (MockStep2AEvent(result=f"{event.result}_A"), MockStep2BEvent(result=f"{event.result}_B"))

  @step()
  async def step_2a(self, events: t.Tuple[MockStep2AEvent]) -> t.Tuple[MockStep3EventA]:
    event = events[0]
    print(f"Step 2A: Processing {event.result}")
    return (MockStep3EventA(combined_result=event.result),)

  @step()
  async def step_3(self, events: t.Tuple[MockStep3EventA, MockStep3EventB]) -> MockEndEvent:
    print(f"Step 3: Joining results - {events[0].combined_result} and {events[1].combined_result}")
    final_result = f"{events[0].combined_result}_{events[1].combined_result}"
    await self.update_context(lambda ctx: MockWorkflowContext(value=final_result))
    return MockEndEvent(run_uuid=self._run_uuid)


async def mock_async_task_failure() -> WorkflowErrorEvent:
  error = LunaException(LunaExceptionCategory.store_workflow_error, description="Mock task failure", cause=None)
  return WorkflowErrorEvent(run_uuid=uuid7(), exception=error, errored_events=None, step_name=Some("mock_async_task_failure"), workflow_name="MockWorkflow")


class MockWorkflowWithSubtask(MockWorkflow):
  """A simple mock workflow with a wait for subtask event in a step."""

  @step()
  async def step_with_subtask(self, events: t.Tuple[MockStep1Event]) -> WorkflowWaitForSubtaskEvent[t.Tuple[MockStep2AEvent]] | WorkflowErrorEvent:
    async def mock_async_task_success() -> t.Tuple[MockStep2AEvent]:
      # this is a mock async subtask
      return (MockStep2AEvent(result="success"),)

    return WorkflowWaitForSubtaskEvent(run_uuid=self._run_uuid, task=mock_async_task_success, timeout_seconds=5.0)

  @step()
  async def step_with_subtask_success(self, events: t.Tuple[MockStep2AEvent]) -> MockEndEvent:
    return MockEndEvent(run_uuid=self._run_uuid)

  @step()
  async def step_with_subtask_failure(self, events: t.Tuple[MockStep2BEvent]) -> WorkflowWaitForSubtaskEvent[t.Tuple[MockStep3EventA]] | WorkflowErrorEvent:
    return WorkflowWaitForSubtaskEvent(run_uuid=self._run_uuid, task=mock_async_task_failure, timeout_seconds=5.0)


class MockComponentContext(WorkflowContextBase):
  value: str = Field(default="")


class MockComponentEvent1(WorkflowEventBase):
  data: str


class MockComponentEvent2(WorkflowEventBase):
  data: str


class MockComponentStartWorkflow(BaseModel):
  initial_value: str


class MockComponentsWorkflow(Workflow[MockComponentContext, MockComponentStartWorkflow, MockEndEvent]):
  async def start(self, payload: MockComponentStartWorkflow) -> t.Tuple[MockComponentEvent1]:
    return (MockComponentEvent1(data=payload.initial_value),)

  @step()
  async def step_1(self, events: t.Tuple[MockComponentEvent1]) -> t.Tuple[MockComponentEvent2] | WorkflowErrorEvent:
    return (MockComponentEvent2(data=events[0].data + "_processed"),)

  @step()
  async def step_2(self, events: t.Tuple[MockComponentEvent2]) -> WorkflowEndEventBase | WorkflowErrorEvent:
    return WorkflowEndEventBase(run_uuid=self._run_uuid)


class MockWorkflowWithRetry(Workflow[MockWorkflowContext, MockStartWorkflowEvent, MockEndEvent]):
  attempt_count = 0

  async def start(self, payload: MockStartWorkflowEvent) -> t.Tuple[MockStep1Event]:
    return (MockStep1Event(result=payload.initial_value),)

  @step(max_retry_attempts=2)
  async def step_with_retry(self, events: t.Tuple[MockStep1Event]) -> t.Tuple[MockStep2AEvent] | WorkflowErrorEvent:
    self.attempt_count += 1
    if self.attempt_count <= 2:
      return self.workflow_error_event(LunaException(LunaExceptionCategory.store_workflow_error, description=f"Retry attempt {self.attempt_count}", cause=None))
    return (MockStep2AEvent(result=f"{events[0].result}_success"),)

  @step()
  async def final_step(self, events: t.Tuple[MockStep2AEvent]) -> MockEndEvent:
    return MockEndEvent(run_uuid=self._run_uuid)
