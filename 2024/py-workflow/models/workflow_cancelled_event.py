from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from shared.services.workflow.models.workflow_event_base import WorkflowEventKind


class WorkflowCancelledEvent(BaseModel):
  """An event that is emitted when a workflow is cancelled by consumer with asyncio.CancelledError."""

  model_config = ConfigDict(frozen=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.cancelled)
  run_uuid: UUID
