import typing as t

import pytest
from lagom import Container
from shared.services.workflow.__test__.conftest import (
  MockStartWorkflowEvent,
  MockStep2AEvent,
  MockWorkflow,
  MockWorkflowContext,
  MockWorkflowWithSubtask,
)
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_events_union import AllWorkflowEventsUnion
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus
from uuid_utils.compat import uuid7


@pytest.fixture
def mock_workflow(mock_dependency_container: Container) -> MockWorkflow:
  return MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.fixture
def mock_workflow_with_subtask(mock_dependency_container: Container) -> MockWorkflow:
  return MockWorkflowWithSubtask(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.mark.asyncio
async def test_workflow_with_subtask(mock_workflow_with_subtask: MockWorkflow) -> None:
  start_event = MockStartWorkflowEvent(initial_value="test")

  results: t.List[AllWorkflowEventsUnion] = []
  async for result in mock_workflow_with_subtask.start_run(start_event):
    results.append(result)

  # Debugging: Print the types of the results
  print([type(r) for r in results])

  # Check if any tuple's first element is an instance of MockStep2AEvent
  assert any(isinstance(r[0], MockStep2AEvent) for r in results if isinstance(r, tuple)), "Expected a MockStep2AEvent in the results"

  end_event = mock_workflow_with_subtask.get_run_result().end_event.some
  assert isinstance(end_event, WorkflowEndEventBase), f"Expected end_event to be WorkflowEndEventBase, but got {type(end_event)}"
  assert mock_workflow_with_subtask.get_run_result().status == WorkflowRunStatus.completed
