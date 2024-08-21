import typing as t

from shared.services.workflow.models.workflow_cancelled_event import WorkflowCancelledEvent
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase
from shared.services.workflow.models.workflow_retry_event import WorkflowRetryEvent
from shared.services.workflow.models.workflow_wait_for_Subtask_event import WorkflowWaitForSubtaskEvent

AllWorkflowEventsUnion: t.TypeAlias = (
  t.Tuple[WorkflowEventBase, ...]
  | WorkflowErrorEvent
  | WorkflowCancelledEvent
  | WorkflowEndEventBase
  | WorkflowRetryEvent
  | WorkflowWaitForSubtaskEvent[t.Tuple[WorkflowEventBase, ...]]
)


WorkflowSpecialEventsUnion: t.TypeAlias = (
  WorkflowEndEventBase | WorkflowRetryEvent | WorkflowWaitForSubtaskEvent[t.Tuple[WorkflowEventBase, ...]] | WorkflowErrorEvent
)
