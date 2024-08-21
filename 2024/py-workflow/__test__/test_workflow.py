import asyncio
import typing as t

import pytest
from lagom import Container
from shared.services.workflow.__test__.conftest import (
  MockEndEvent,
  MockStartWorkflowEvent,
  MockStep1Event,
  MockStep2AEvent,
  MockStep2BEvent,
  MockStep3EventA,
  MockStep3EventB,
  MockWorkflow,
  MockWorkflowContext,
)
from shared.services.workflow.models.workflow_cancelled_event import WorkflowCancelledEvent
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_events_union import AllWorkflowEventsUnion
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus
from uuid_utils.compat import uuid7


@pytest.fixture
def mock_workflow(mock_dependency_container: Container) -> MockWorkflow:
  return MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.mark.asyncio
async def test_complete_workflow_execution(mock_workflow: MockWorkflow) -> None:
  start_event = MockStartWorkflowEvent(initial_value="test")

  results: t.List[AllWorkflowEventsUnion] = []
  assert (
    mock_workflow.get_run_result().status == WorkflowRunStatus.not_started
  ), f"Expected initial status to be not_started, but got {mock_workflow.get_run_result().status}"
  async for result in mock_workflow.start_run(start_event):
    if isinstance(result, tuple):
      assert (
        mock_workflow.get_run_result().status == WorkflowRunStatus.in_progress
      ), f"Expected status to be in_progress during execution, but got {mock_workflow.get_run_result().status}"
    results.append(result)

  assert len(results) == 5, f"Expected 5 results, but got {len(results)}"
  assert any(isinstance(r, WorkflowEndEventBase) for r in results), "Expected a WorkflowEndEventBase in the results"
  end_event = mock_workflow.get_run_result().end_event.some
  assert (
    mock_workflow.get_run_result().status == WorkflowRunStatus.completed
  ), f"Expected final status to be completed, but got {mock_workflow.get_run_result().status}"
  assert isinstance(end_event, WorkflowEndEventBase), f"Expected end_event to be WorkflowEndEventBase, but got {type(end_event)}"
  assert mock_workflow.context.value == "test_A_test_B", f"Expected context value to be 'test_A_test_B', but got {mock_workflow.context.value}"


@pytest.mark.asyncio
async def test_step_1(mock_workflow: MockWorkflow) -> None:
  event = MockStep1Event(result="test")
  result = await mock_workflow.step_1((event,))
  assert isinstance(result, tuple), f"Expected result to be a tuple, but got {type(result)}"
  assert len(result) == 2, f"Expected 2 events in result, but got {len(result)}"
  assert isinstance(result[0], MockStep2AEvent), f"Expected first event to be MockStep2AEvent, but got {type(result[0])}"
  assert isinstance(result[1], MockStep2BEvent), f"Expected second event to be MockStep2BEvent, but got {type(result[1])}"


@pytest.mark.asyncio
async def test_step_2a(mock_workflow: MockWorkflow) -> None:
  event = MockStep2AEvent(result="test_A")
  result = await mock_workflow.step_2a((event,))
  assert isinstance(result, tuple)
  assert len(result) == 1
  assert isinstance(result[0], MockStep3EventA)


@pytest.mark.asyncio
async def test_step_2b(mock_workflow: MockWorkflow) -> None:
  event = MockStep2BEvent(result="test_B")
  result = await mock_workflow.step_2b((event,))
  assert isinstance(result, tuple)
  assert len(result) == 1
  assert isinstance(result[0], MockStep3EventB)


@pytest.mark.asyncio
async def test_step_3(mock_workflow: MockWorkflow) -> None:
  event_a = MockStep3EventA(combined_result="test_A")
  event_b = MockStep3EventB(combined_result="test_B")
  result = await mock_workflow.step_3((event_a, event_b))

  assert isinstance(result, MockEndEvent)
  assert mock_workflow.context.value == "test_A_test_B"


@pytest.mark.asyncio
async def test_workflow_cancellation(mock_workflow: MockWorkflow) -> None:
  start_event = MockStartWorkflowEvent(initial_value="test")

  results: t.List[AllWorkflowEventsUnion] = []
  async for result in mock_workflow.start_run(start_event):
    results.append(result)
    await mock_workflow.cancel()

  end_event = mock_workflow.get_run_result().end_event.some
  assert isinstance(end_event, WorkflowCancelledEvent)
  assert mock_workflow.get_run_result().status == WorkflowRunStatus.completed


@pytest.mark.asyncio
async def test_multiple_workflows(mock_dependency_container: Container) -> None:
  workflow1 = MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)
  workflow2 = MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)

  start_event1 = MockStartWorkflowEvent(initial_value="test1")
  start_event2 = MockStartWorkflowEvent(initial_value="test2")

  async def run_workflow(workflow: MockWorkflow, start_event: MockStartWorkflowEvent) -> None:
    async for _ in workflow.start_run(start_event):
      pass

  await asyncio.gather(run_workflow(workflow1, start_event1), run_workflow(workflow2, start_event2))

  assert workflow1.context.value == "test1_A_test1_B", f"Expected workflow1 context value to be 'test1_A_test1_B', but got '{workflow1.context.value}'"
  assert workflow2.context.value == "test2_A_test2_B", f"Expected workflow2 context value to be 'test2_A_test2_B', but got '{workflow2.context.value}'"


@pytest.mark.asyncio
async def test_workflow_execute_until_end(mock_workflow: MockWorkflow) -> None:
  start_event = MockStartWorkflowEvent(initial_value="test")

  result = await mock_workflow.run_until_end(start_event)
  assert result.is_ok(), f"Expected result to be Ok, but got Error: {result.error if result.is_error() else 'Unknown error'}"
  assert isinstance(result.ok, WorkflowEndEventBase), f"Expected result.ok to be WorkflowEndEventBase, but got {type(result.ok)}"
