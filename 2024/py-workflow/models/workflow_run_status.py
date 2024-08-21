from enum import Enum


class WorkflowRunStatus(str, Enum):
  not_started = "not_started"
  in_progress = "in_progress"
  failed = "failed"
  completed = "completed"
