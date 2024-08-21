from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from shared.services.workflow.models.workflow_event_base import WorkflowEventKind


class WorkflowEndEventBase(BaseModel):
  """The base class for all end events that wrap up a workflow."""

  model_config = ConfigDict(arbitrary_types_allowed=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.end)
  run_uuid: UUID
