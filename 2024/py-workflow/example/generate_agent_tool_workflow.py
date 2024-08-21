# This is a partial example that may reference code not in the example file.
# Shows you how to use and create a workflow

import typing as t
from textwrap import dedent
from uuid import UUID

from agent_store.services.agent_message.agent_message_service import AgentMessageService
from agent_store.services.agent_message.models.agent_message_call import AgentMessageCallType
from agent_store.services.agent_tools.agent_tool_adapter import AgentToolAdapter
from agent_store.services.agent_tools.agent_tool_service import AgentToolService
from agent_store.services.agent_tools.models.agent_tool import AgentTool
from agent_store.services.agent_tools.models.agent_tool_configuration import AgentToolConfigurationUnion, PythonModuleToolConfiguration
from agent_store.services.agent_tools.models.agent_tool_configuration_source import AgentToolConfigurationSource
from agent_store.services.agent_tools.models.prompts.agent_tool_call_prompt_template import AgentToolCallPromptInput, AgentToolCallPromptTemplate
from agent_store.services.agent_tools.models.prompts.agent_tool_description_introspection_prompt_templates import (
  AgentToolDescriptionIntrospectionPromptTemplate,
  AgentToolDescriptionIntrospectionResponse,
)
from agent_store.services.agent_tools.models.prompts.agent_tool_introspection_prompt_input import AgentToolIntrospectionPromptInput
from agent_store.services.agent_tools.models.prompts.agent_tool_manual_introspection_prompt_template import (
  AgentToolManualIntrospectionPromptTemplate,
  AgentToolManualIntrospectionResponse,
)
from agent_store.services.llm.models.llm_call_params import LlmCallParams, LlmModels, LlmProviders
from expression import Nothing, Ok, Option, Result, Some
from pydantic import ConfigDict, Field
from shared.common.functions.otel_helpers import current_span_id
from shared.common.models.luna_exception import LunaException
from shared.common.models.luna_exception_category import LunaExceptionCategory
from shared.services.workflow.models.workflow_context_base import WorkflowContextBase
from shared.services.workflow.models.workflow_end_event_base import WorkflowEndEventBase
from shared.services.workflow.models.workflow_error_event import WorkflowErrorEvent
from shared.services.workflow.models.workflow_event_base import WorkflowEventBase
from shared.services.workflow.workflow import Workflow
from shared.services.workflow.workflow_step_decorator import step
from uuid_utils.compat import uuid7


class GenerateAgentToolWorkflowContext(WorkflowContextBase):
  model_config = ConfigDict(arbitrary_types_allowed=True)

  tenant_uuid: UUID
  partial_agent_tool: Option[AgentTool] = Field(default=Nothing)
  tool_configuration: PythonModuleToolConfiguration
  tool_source: AgentToolConfigurationSource


class Step1CheckToolExistsEvent(WorkflowEventBase):
  pass


class Step1CallToolApiForOutputExampleEvent(WorkflowEventBase):
  pass


class Step2CreateToolDescriptionEvent(WorkflowEventBase):
  tool_output: str


class Step2CreateToolManualEvent(WorkflowEventBase):
  tool_output: str


class Step3PersistDescriptionEvent(WorkflowEventBase):
  description_response: Result[AgentToolDescriptionIntrospectionResponse, LunaException]


class Step3PersistManualEvent(WorkflowEventBase):
  manual_response: Result[AgentToolManualIntrospectionResponse, LunaException]


class GenerateAgentToolEndEvent(WorkflowEndEventBase):
  agent_tool: Result[AgentTool, LunaException]
  already_exists: bool


class GenerateAgentToolWorkflow(Workflow[GenerateAgentToolWorkflowContext, None, GenerateAgentToolEndEvent]):
  """Generating a new agent tool.
  ```mermaid
  graph TD
    A[Start] --> B[create_partial_tool]
    B -->|Tool exists| C[End: Tool already exists]
    B -->|Tool doesn't exist| D[call_tool_api_for_output_example]
    D --> E[create_tool_description]
    D --> F[create_tool_manual]
    E --> G[persist_tool]
    F --> G
    G -->|Success| H[End: New tool created]
    G -->|Failure| I[End: Error]
  ```

  """

  async def start(
    self,
    payload: None,
  ) -> t.Tuple[Step1CheckToolExistsEvent]:
    return (Step1CheckToolExistsEvent(),)

  @step()
  async def create_partial_tool(
    self, events: t.Tuple[Step1CheckToolExistsEvent, ...]
  ) -> t.Tuple[Step1CallToolApiForOutputExampleEvent] | GenerateAgentToolEndEvent | WorkflowErrorEvent:
    agent_tool_service = self._container[AgentToolService]

    partial_module_tool = AgentTool(
      tenant_uuid=self.context.tenant_uuid,
      name=self.context.tool_configuration.name,
      source=self.context.tool_source,
      tool_configuration=AgentToolConfigurationUnion(root=self.context.tool_configuration),
      digest=self.context.tool_configuration.digest(),
      description="",
    )

    exists = await agent_tool_service.agent_tool_exists(partial_module_tool)
    if exists.is_ok() and exists.ok.is_some():
      return GenerateAgentToolEndEvent(already_exists=True, agent_tool=Ok(exists.ok.some), run_uuid=self._run_uuid)
    elif exists.is_error():
      return self.workflow_error_event(exists.error)
    else:

      def update_context(c: GenerateAgentToolWorkflowContext) -> GenerateAgentToolWorkflowContext:
        c.partial_agent_tool = Some(partial_module_tool)
        return c

      await self.update_context(update_context)
      return (Step1CallToolApiForOutputExampleEvent(),)

  @step(max_retry_attempts=1)
  async def call_tool_api_for_output_example(
    self, events: t.Tuple[Step1CallToolApiForOutputExampleEvent, ...]
  ) -> t.Tuple[Step2CreateToolManualEvent, Step2CreateToolDescriptionEvent] | WorkflowErrorEvent:
    message_service = self._container[AgentMessageService]

    tool_output_as_str = ""
    # 1. call the tool with example input to get example output
    if self.context.partial_agent_tool.is_some():
      tool = self.context.partial_agent_tool.some
      tool_adapter = AgentToolAdapter.create_adapter(tool)
      function_params_model = tool_adapter.ok.fn_input_model

      input = AgentToolCallPromptInput(
        agent_tool=tool,
        tool_input_request=dedent("""\
        Please call this function with realistic example input parameters.   You can use popular input parameters and symbols.  Examples could be periods = annual or quarterly; etc...  We want to use the response to understand the tool output and behaviour.  Below are some hints to help you, but only use them if they are relevant please.
        
        Example hints:
        - Company ticker symbol: AAPL
        - Period: annual, quaterly
        - Commodities ticker symbol: CL (crude oil)
        - Futures ticker symbol: ES (e-mini S&P 500)
        - Statistics: sma, rsi
        - SEC CIK: 
          - [The Central Index Key (CIK) is used on the SEC's computer systems to identify corporations and individual people who have filed disclosure with the SEC]
          - 0000320193 (AAPL) 
          - 1534083 (Warren Buffet)
        - limit: 10 or below to reduce cost
        - download: we do not want to download any data for these example request. A falsy value is recommended.
        ...
        """),
        prior_attempt_error=events[0].prior_attempt_error,
      )
      fn_input_response = await message_service.send_message_call(
        AgentToolCallPromptTemplate,
        input,
        template_response_cls_params=function_params_model,
        call_type=AgentMessageCallType.system_call,
        tenant_uuid=self.context.tenant_uuid,
        call_graph_uuid=uuid7(),
        call_params=LlmCallParams(provider=LlmProviders.openai, model=LlmModels.openai_medium),
        send_instrumentation=True,
        max_retries=10,
        parent_span_id=current_span_id(),
      )

      if fn_input_response.is_error():
        fn_input_response.error.prepend_description("Error calling agent tool for example output")
        return self.workflow_error_event(fn_input_response.error)

      if fn_input_response.is_ok():
        input_params = tool_adapter.ok.validate_fn_input(fn_input_response.ok["input_params"])
        tool_output = tool_adapter.ok.call_func(input_params)

        if tool_output.is_error():
          tool_output.error.prepend_description("Error calling agent tool for example output")
          return self.workflow_error_event(tool_output.error)

        tool_output_as_str = str(tool_output.ok)[:200] + ("\n..." if len(str(tool_output)) > 200 else "")

        return (Step2CreateToolManualEvent(tool_output=tool_output_as_str), Step2CreateToolDescriptionEvent(tool_output=tool_output_as_str))

    return self.workflow_error_event(
      exception=LunaException(
        LunaExceptionCategory.store_workflow_error,
        description="Error calling agent tool for example output",
        cause=None,
        details={"tool_configuration": self.context.tool_configuration, "partial_tool": self.context.partial_agent_tool},
      ),
    )

  @step()
  async def create_tool_description(
    self, events: t.Tuple[Step2CreateToolDescriptionEvent, ...]
  ) -> t.Tuple[Step3PersistDescriptionEvent, ...] | WorkflowErrorEvent:
    message_service = self._container[AgentMessageService]

    tool_output = events[0].tool_output

    response = await message_service.send_message_call(
      AgentToolDescriptionIntrospectionPromptTemplate,
      AgentToolIntrospectionPromptInput(tool_configuration=self.context.tool_configuration, example_output=tool_output),
      call_type=AgentMessageCallType.system_call,
      tenant_uuid=self.context.tenant_uuid,
      call_graph_uuid=uuid7(),
      call_params=LlmCallParams(provider=LlmProviders.openai, model=LlmModels.openai_medium),
      send_instrumentation=True,
      max_retries=10,
      parent_span_id=current_span_id(),
    )

    return (Step3PersistDescriptionEvent(description_response=response),)

  @step()
  async def create_tool_manual(self, events: t.Tuple[Step2CreateToolManualEvent, ...]) -> t.Tuple[Step3PersistManualEvent, ...] | WorkflowErrorEvent:
    message_service = self._container[AgentMessageService]

    tool_output = events[0].tool_output

    # Create the manual
    introspection_response = await message_service.send_message_call(
      AgentToolManualIntrospectionPromptTemplate,
      AgentToolIntrospectionPromptInput(tool_configuration=self.context.tool_configuration, example_output=tool_output),
      call_type=AgentMessageCallType.system_call,
      tenant_uuid=self.context.tenant_uuid,
      call_graph_uuid=uuid7(),
      call_params=LlmCallParams(provider=LlmProviders.openai, model=LlmModels.openai_medium),
      send_instrumentation=True,
      max_retries=10,
      parent_span_id=current_span_id(),
    )

    return (Step3PersistManualEvent(manual_response=introspection_response),)

  @step(max_retry_attempts=1)
  async def persist_tool(self, events: t.Tuple[Step3PersistDescriptionEvent, Step3PersistManualEvent]) -> GenerateAgentToolEndEvent | WorkflowErrorEvent:
    manual_response = events[1].manual_response
    description_response = events[0].description_response

    partial_module_tool = self.context.partial_agent_tool

    if manual_response.is_ok() and description_response.is_ok() and partial_module_tool.is_some():
      # fill in the description and tags
      full_module_tool: AgentTool = description_response.ok.assign_fields(partial_module_tool.some)
      full_module_tool = manual_response.ok.assign_fields(full_module_tool)

      agent_tool_service = self._container[AgentToolService]
      agent_tool = await agent_tool_service.upsert_agent_tool(full_module_tool)

      if agent_tool.is_ok():
        return GenerateAgentToolEndEvent(already_exists=False, agent_tool=Ok(agent_tool.ok), run_uuid=self._run_uuid)

      return self.workflow_error_event(agent_tool.error)

    return self.workflow_error_event(
      exception=LunaException(
        LunaExceptionCategory.store_workflow_error,
        description="Error creating agent tool and persisting it",
        details={"partial_tool": partial_module_tool, "events": events, "manual_response": manual_response, "description_response": description_response},
        cause=manual_response.error or description_response.error or None,
      ),
    )
