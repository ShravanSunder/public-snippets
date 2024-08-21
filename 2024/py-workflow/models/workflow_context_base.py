from pydantic import BaseModel, ConfigDict


class WorkflowContextBase(BaseModel):
  """
  The base context used by the workflow.
  """

  model_config = ConfigDict(extra="forbid")
