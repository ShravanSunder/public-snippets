import logging
from enum import Enum
from uuid import UUID

from expression import Nothing, Option
from pydantic import BaseModel, ConfigDict, Field
from shared.common.models.luna_exception import LunaException
from uuid_utils.compat import uuid7

logger = logging.getLogger(__name__)


class WorkflowEventKind(str, Enum):
  event = "event"
  end = "end"
  cancelled = "cancelled"
  error = "error"
  retry = "retry"
  invoke = "invoke"
  timeout = "timeout"


class WorkflowEventBase(BaseModel):
  """The base class for all step events except EndEvent."""

  model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

  kind: WorkflowEventKind = Field(default=WorkflowEventKind.event)
  event_uuid: UUID = Field(default_factory=uuid7)

  attempt_number: int = Field(default=0)
  prior_attempt_error: Option[LunaException] = Field(default=Nothing)

  @classmethod
  def cls_hash_key(cls) -> str:
    return cls.__qualname__
