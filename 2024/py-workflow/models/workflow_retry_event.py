import typing as t
from uuid import UUID

from expression import Nothing, Option
from pydantic import BaseModel, ConfigDict, Field
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase, WorkflowEventKind


class WorkflowRetryEvent(BaseModel):
  """An internal event that tells the workflow logic to retry the current step if @step() decorator retry is set to true."""

  model_config = ConfigDict(arbitrary_types_allowed=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.retry)
  current_step: Option[str] = Field(default=Nothing)
  run_uuid: UUID
  workflow_error_event: WorkflowErrorEvent
  retry_events: t.Tuple[WorkflowEventBase, ...] = Field()
  attempt_number: int = Field()

  def __str__(self) -> str:
    return f"WorkflowRetryEvent(workflow_name={self.workflow_error_event.workflow_name}, run_uuid={self.run_uuid}, step_name={self.current_step}, attempt_number={self.attempt_number})"
