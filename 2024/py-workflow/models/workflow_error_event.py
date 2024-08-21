import typing as t
from uuid import UUID

from expression import Nothing, Option
from pydantic import BaseModel, ConfigDict, Field
from shared.common.models.luna_exception import LunaException
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase, WorkflowEventKind

data: Option[t.Tuple[WorkflowEventBase, ...]] = Nothing


class WorkflowErrorEvent(BaseModel):
  model_config = ConfigDict(arbitrary_types_allowed=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.error)
  exception: LunaException
  run_uuid: UUID

  errored_events: t.Tuple[WorkflowEventBase, ...] | None = Field()
  # None is used instead of Option because of Pydantic's handling of Option with tuple fields and errors
  step_name: Option[str] = Field()
  workflow_name: str = Field()
  attempt_number: int = Field(default=0)

  def __str__(self) -> str:
    return f"WorkflowErrorEvent(workflow_name={self.workflow_name}, run_uuid={self.run_uuid}, step_name={self.step_name}, exception={self.exception})"

  def to_exception(self) -> LunaException:
    exception = self.exception

    step_name = self.step_name.some if self.step_name.is_some() else "Unknown"

    exception.add_details(
      {
        "workflow_name": self.workflow_name,
        "run_uuid": self.run_uuid,
        "step_fn_name": step_name,
        "errored_events": self.errored_events,
      }
    )
    exception.description = f"Workflow: {self.workflow_name} error in step: {step_name}:: {self.exception.description}"
    return exception


class WorkflowTimeoutEvent(WorkflowErrorEvent):
  model_config = ConfigDict(frozen=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.timeout)


class WorkflowAlreadyRunningException(LunaException):
  pass
