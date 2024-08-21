# In-process Workflow Framework

The Workflow framework is a useful system creating structured, event-driven workflows in Python.  This is a inprocess non distributed workflow system with retries, waits for external asyncronous tasks and fully async steps.  

It simplifies the process of defining complex business logic with clear steps, error handling, and retry mechanisms.

Its meant to be used for Gen AI workflows that invovle multiple steps, retries, and asyncronous tasks.  These tasks are I/O bound and async works well for this.

> Note the snippet might reference imports that don't exist for this showcase

## Key Features

- Event-driven architecture
- Automatic step registration using the `@step()` decorator
- Built-in error handling and retry logic
- Concurrent execution of steps
- Context management for workflow state

## How It Works

1. Define a workflow class that inherits from `Workflow[ContextType, StartPayload, EndEventType]`.
2. Implement the `start` method to initiate the workflow.
3. Define steps using the `@step()` decorator within the workflow class.
4. Each step receives events and returns new events or error events.
5. The workflow engine manages the flow between steps based on the events produced.

## Example Usage

```python
class MyWorkflow(Workflow[MyContext, MyStartEvent, MyEndEvent]):
    async def start(self, payload: MyStartEvent) -> Tuple[MyFirstStepEvent]:
        return (MyFirstStepEvent(data=payload.initial_data),)

    @step()
    async def first_step(self, events: Tuple[MyFirstStepEvent]) -> Tuple[MySecondStepEvent] | WorkflowErrorEvent:
        # Process first step
        return (MySecondStepEvent(result=process_data(events[0].data)),)

    @step()
    async def second_step(self, events: Tuple[MySecondStepEvent]) -> MyEndEvent | WorkflowErrorEvent:
        # Process second step
        return MyEndEvent(final_result=finalize_data(events[0].result))
```

This example demonstrates a simple workflow with two steps. It shows how to:
- Define a workflow class
- Implement the `start` method
- Use the `@step()` decorator to define workflow steps
- Handle events and return appropriate results or error events

## Benefits

- Improved code organization and readability
- Easy to understand and maintain complex business processes
- Built-in error handling and retry mechanisms
- Flexibility to add or modify steps without changing the overall structure
- Facilitates testing of individual workflow steps

The Workflow framework provides a robust structure for implementing complex, multi-step processes in a clear and maintainable way.