from expression import Nothing, Option
from pydantic import BaseModel, ConfigDict, Field
from shared.services.workflow.models.workflow_cancelled_event import WorkflowCancelledEvent
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_run_status import WorkflowRunStatus


class WorkflowRunResult(BaseModel):
  """
  The result of a workflow run.
  """

  model_config = ConfigDict(frozen=True, extra="forbid")
  end_event: Option[WorkflowEndEventBase | WorkflowCancelledEvent | WorkflowErrorEvent] = Field(default=Nothing)
  status: WorkflowRunStatus = Field(default=WorkflowRunStatus.not_started)
