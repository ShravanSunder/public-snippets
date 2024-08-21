import logging
import typing as t
from functools import wraps
from inspect import signature

from expression import Some
from shared.common.models.luna_exception import LunaException
from shared.common.models.luna_exception_category import LunaExceptionCategory
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import (
  WorkflowEventBase,
)
from shared.services.workflow.models.workflow_wait_for_Subtask_event import WorkflowWaitForSubtaskEvent
from shared.services.workflow.workflow import Workflow
from shared.services.workflow.workflow_step import StepConfig

logger = logging.getLogger()


_TWorkflowSelf = t.TypeVar("_TWorkflowSelf", bound=Workflow[t.Any, t.Any, t.Any])
_TWorkflowEventInput = t.TypeVar("_TWorkflowEventInput", bound=t.Tuple[WorkflowEventBase, ...])
_TWorkflowEventReturn = t.TypeVar(
  "_TWorkflowEventReturn",
  bound=t.Tuple[WorkflowEventBase, ...],
  covariant=True,
)


def step(*, step_timeout_seconds: float = 180, max_retry_attempts: int = 0):
  """
  Decorator to define a step function in a workflow class.  Fn should take a tuple of events as the first parameter and return a tuple of events.

  Args:
      step_timeout_seconds: The number of seconds to wait for the step to complete before timing out. Defaults to 120.
  """

  def decorator(
    func: t.Callable[
      [_TWorkflowSelf, _TWorkflowEventInput],
      t.Awaitable[_TWorkflowEventReturn | WorkflowErrorEvent | WorkflowEndEventBase | WorkflowWaitForSubtaskEvent[_TWorkflowEventReturn]],
    ],
  ) -> t.Callable[
    [_TWorkflowSelf, _TWorkflowEventInput],
    t.Awaitable[_TWorkflowEventReturn | WorkflowErrorEvent | WorkflowEndEventBase | WorkflowWaitForSubtaskEvent[_TWorkflowEventReturn]],
  ]:
    trigger_events_cls = extract_trigger_event_classes(func)
    config = StepConfig(
      trigger_event_definitions=trigger_events_cls,
      step_timeout_seconds=step_timeout_seconds,
      max_retry_attempts=max_retry_attempts,
    )
    config.step_name = func.__name__

    @wraps(func)
    async def wrap_fn(
      self: _TWorkflowSelf, step_event: _TWorkflowEventInput
    ) -> _TWorkflowEventReturn | WorkflowErrorEvent | WorkflowWaitForSubtaskEvent[_TWorkflowEventReturn] | WorkflowEndEventBase:
      if config.workflow_name is None:  # type: ignore
        config.workflow_name = self.__class__.__name__

      # todo add pre logging etc
      result = await func(self, step_event)
      # todo add post logging etc

      if isinstance(result, WorkflowErrorEvent):
        result.step_name = Some(config.step_name)
        result.run_uuid = self.run_uuid
        result.workflow_name = config.workflow_name
        result.errored_events = step_event
      return result

    setattr(
      wrap_fn,
      "_StepConfig",
      config,
    )

    return wrap_fn

  return decorator


def extract_trigger_event_classes(
  func: t.Callable[
    [_TWorkflowSelf, _TWorkflowEventInput],
    t.Awaitable[_TWorkflowEventReturn | WorkflowEndEventBase | WorkflowErrorEvent | WorkflowWaitForSubtaskEvent[_TWorkflowEventReturn]],
  ],
) -> t.Tuple[t.Type[WorkflowEventBase], ...]:
  """
  Extract the trigger event classes from a function parameter's type annotation.

  This function expects the parameter to be annotated with a Tuple of StepEventBase subclasses.
  For example: Tuple[Event1, Event2] or Tuple[Event1, ...].

  Args:
      event_param (Parameter): The parameter object from a function's signature.

  Returns:
      Tuple[Type[StepEventBase], ...]: A tuple of StepEventBase subclasses.

  Raises:
      LunaException: If the annotation is not a Tuple or if the types within the Tuple
      are not subclasses of StepEventBase.
  """
  try:
    sig = signature(func)
    event_param = list(sig.parameters.values())[1]  # Get the second parameter (first is 'self')
    # Get the annotation of the trigger event (it should be a tuple type)
    trigger_event_tuple_type = event_param.annotation

    # Check if the annotation is a Tuple
    if not (hasattr(trigger_event_tuple_type, "__origin__") and trigger_event_tuple_type.__origin__ is tuple):
      raise ValueError("Event parameter must be annotated with a Tuple type.")

    # Get the class types in the tuple
    trigger_events_cls = t.get_args(trigger_event_tuple_type)

    # Remove Ellipsis if present (for cases like Tuple[Event, ...])
    trigger_events_cls = tuple(cls for cls in trigger_events_cls if cls is not Ellipsis)

    # Validate that all classes are subclasses of StepEventBase
    if not all(issubclass(cls, WorkflowEventBase) for cls in trigger_events_cls):
      raise ValueError("All event classes must be subclasses of StepEventBase.")

    return trigger_events_cls

  except Exception as e:
    raise LunaException(
      category=LunaExceptionCategory.store_workflow_error,
      description="Failed to extract trigger event classes from step function parameter.",
      cause=e,
      details={"func": func, "func_name": func.__name__, "func_str": str(func)},
    ) from e
