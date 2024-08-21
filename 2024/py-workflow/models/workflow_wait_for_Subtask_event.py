import typing as t
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase, WorkflowEventKind


class WorkflowWaitForSubtaskEvent[TWorkflowEvent: t.Tuple[WorkflowEventBase, ...]](BaseModel):
  """Wait for a subtask outside of the normal workflow loop:
  1. execute an arbitrary async function and wait for the result.
  2. then proceed to the next step which is returned by the async function.

  Essentially, this allows you to run other workflows and wait for the result outside of the workflow loop and watchdog loop.
  - The async code must return a WorkflowEvent or WorkflowErrorEvent.
  - This allows you to create a task to run other sub workflows and wait for the result.
  - You can pass in a timeout if you want to limit the time taken to execute the task.
  """

  model_config = ConfigDict(arbitrary_types_allowed=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.invoke)
  run_uuid: UUID
  task: t.Callable[[], t.Coroutine[t.Any, t.Any, TWorkflowEvent | WorkflowErrorEvent]]
  timeout_seconds: float | None = Field(default=None)

  def __str__(self) -> str:
    return f"WorkflowExecuteSubtaskEvent(run_uuid={self.run_uuid}, task={self.task.__name__}, timeout_seconds={self.timeout_seconds})"
