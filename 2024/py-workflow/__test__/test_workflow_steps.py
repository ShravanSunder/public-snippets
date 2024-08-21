# pyright: reportPrivateUsage=false

from uuid import uuid4

import pytest
from lagom import Container
from shared.services.workflow.__test__.conftest import MockComponentContext, MockComponentEvent1, MockComponentEvent2, MockComponentsWorkflow


@pytest.fixture
def mock_workflow(mock_dependency_container: Container) -> MockComponentsWorkflow:
  return MockComponentsWorkflow(uuid4(), context=MockComponentContext(), container=mock_dependency_container)


@pytest.mark.asyncio
async def test_workflow_step_process_inbox(mock_workflow: MockComponentsWorkflow):
  step = mock_workflow._step_fn_registry["step_1"]
  run_uuid = uuid4()
  step.init_run_queues(run_uuid)

  await step.put_event(MockComponentEvent1(data="test"), run_uuid)
  queue = step._run_queues[run_uuid]
  assert queue.inbox.qsize() == 1
  await step.process_inbox(run_uuid)
  assert queue.ready_to_execute.__len__() == 1

  assert isinstance(queue.ready_to_execute[0][0], MockComponentEvent1)


@pytest.mark.asyncio
async def test_workflow_register_steps(mock_workflow: MockComponentsWorkflow):
  assert "step_1" in MockComponentsWorkflow._step_fn_registry
  assert "step_2" in MockComponentsWorkflow._step_fn_registry
  assert MockComponentEvent1.cls_hash_key() in MockComponentsWorkflow._event_hash_to_step_fn_mapping
  assert MockComponentEvent2.cls_hash_key() in MockComponentsWorkflow._event_hash_to_step_fn_mapping


@pytest.mark.asyncio
async def test_workflow_send_event(mock_workflow: MockComponentsWorkflow):
  event = MockComponentEvent1(data="test")
  await mock_workflow.send_event(event)
  step = MockComponentsWorkflow._event_hash_to_step_fn_mapping[MockComponentEvent1.cls_hash_key()]
  assert step._run_queues[mock_workflow._run_uuid].inbox.qsize() == 1
