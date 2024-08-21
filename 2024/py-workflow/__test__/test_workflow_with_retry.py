import typing as t

import pytest
from lagom import Container
from shared.services.workflow.__test__.conftest import (
  MockEndEvent,
  MockStartWorkflowEvent,
  MockStep2AEvent,
  MockWorkflow,
  MockWorkflowContext,
  MockWorkflowWithRetry,
)
from shared.services.workflow.models.workflow_events_union import AllWorkflowEventsUnion
from shared.services.workflow.models.workflow_retry_event import WorkflowRetryEvent
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus
from uuid_utils.compat import uuid7


@pytest.fixture
def mock_workflow_with_retry(mock_dependency_container: Container) -> MockWorkflowWithRetry:
  return MockWorkflowWithRetry(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.fixture
def mock_workflow(mock_dependency_container: Container) -> MockWorkflow:
  return MockWorkflow(uuid7(), context=MockWorkflowContext(), container=mock_dependency_container)


@pytest.mark.asyncio
async def test_workflow_step_retry(mock_workflow_with_retry: MockWorkflowWithRetry, monkeypatch: pytest.MonkeyPatch) -> None:
  # Mock the backoff_delay method
  async def mock_backoff_delay(self: MockWorkflowWithRetry, event: WorkflowRetryEvent):
    # No actual delay, just log
    print(f"Mocked backoff: Retrying workflow step {event} immediately")

  monkeypatch.setattr(MockWorkflowWithRetry, "backoff_delay", mock_backoff_delay)

  start_event = MockStartWorkflowEvent(initial_value="test")

  results: t.List[AllWorkflowEventsUnion] = []
  async for result in mock_workflow_with_retry.start_run(start_event):
    results.append(result)

  # Check that we have the expected number of events
  assert len(results) == 5, f"Expected 5 results, but got {len(results)}"

  # Check that we have two retry events followed by a successful event
  assert isinstance(results[1], WorkflowRetryEvent), f"Expected first retry, but got {type(results[1])}"
  assert isinstance(results[2], WorkflowRetryEvent), f"Expected second retry, but got {type(results[2])}"
  assert isinstance(results[3], tuple) and isinstance(results[3][0], MockStep2AEvent), f"Expected successful event, but got {type(results[3])}"

  # Check that the final event is the end event
  assert isinstance(results[4], MockEndEvent), f"Expected MockEndEvent, but got {type(results[4])}"

  # Check that the workflow completed successfully
  assert mock_workflow_with_retry.get_run_result().status == WorkflowRunStatus.completed
  assert mock_workflow_with_retry.attempt_count == 3
