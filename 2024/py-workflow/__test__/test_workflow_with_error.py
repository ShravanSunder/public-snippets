import asyncio
import typing as t

import pytest
from lagom import Container
from shared.services.workflow.__test__.conftest import (
  MockStartWorkflowEvent,
  MockStep2BEvent,
  MockWorkflow,
  MockWorkflowContext,
  MockWorkflowWithError,
)
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_events_union import AllWorkflowEventsUnion
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus
from uuid_utils.compat import uuid7


@pytest.fixture
def mock_workflow(mock_dependency_container: Container) -> MockWorkflow:
  return MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.mark.asyncio
async def test_workflow_error_handling(mock_dependency_container: Container) -> None:
  mock_workflow = MockWorkflowWithError(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)
  start_event = MockStartWorkflowEvent(initial_value="trigger_error")

  results: t.List[AllWorkflowEventsUnion] = []
  async for result in mock_workflow.start_run(start_event):
    results.append(result)

  assert any(isinstance(r, WorkflowErrorEvent) for r in results), "Expected a WorkflowErrorEvent in the results"
  end_event = mock_workflow.get_run_result().end_event.some
  assert isinstance(end_event, WorkflowErrorEvent), f"Expected end_event to be WorkflowErrorEvent, but got {type(end_event)}"
  assert (
    mock_workflow.get_run_result().status == WorkflowRunStatus.failed
  ), f"Expected workflow status to be failed, but got {mock_workflow.get_run_result().status}"

  # check exception description
  assert end_event.exception.description == "Test error in step_2b", f"Unexpected error description: {end_event.exception.description}"
  assert (
    end_event.to_exception().description == "Workflow: MockWorkflowWithError error in step: step_2b:: Test error in step_2b"
  ), f"Unexpected to_exception description: {end_event.to_exception().description}"

  # check post processing enrichment
  assert end_event.step_name.some == "step_2b", f"Expected step_name to be 'step_2b', but got {end_event.step_name}"
  assert end_event.workflow_name == "MockWorkflowWithError", f"Expected workflow_name to be 'MockWorkflowWithError', but got {end_event.workflow_name}"
  assert isinstance(end_event.errored_events, tuple), f"Expected errored_events to be a tuple, but got {type(end_event.errored_events)}"
  assert len(end_event.errored_events) == 1, f"Expected 1 errored event, but got {len(end_event.errored_events)}"
  assert isinstance(end_event.errored_events[0], MockStep2BEvent), f"Expected errored event to be MockStep2BEvent, but got {type(end_event.errored_events[0])}"
  assert end_event.run_uuid == mock_workflow.run_uuid, f"Expected run_uuid to match, but got {end_event.run_uuid} instead of {mock_workflow.run_uuid}"


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
