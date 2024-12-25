import queue
import time
from typing import Annotated
import uuid

import click
from fastapi import Depends

from nextdata.cli.dev_server.backend.deps.get_main_thread_queues import (
    get_main_thread_queues,
)
from nextdata.cli.types import StackOutputs


def get_stack_outputs(
    queues: Annotated[tuple[queue.Queue, queue.Queue], Depends(get_main_thread_queues)]
) -> StackOutputs:
    """Get stack outputs from the main thread"""
    resource_request_queue, resource_response_queue = queues
    request_id = str(uuid.uuid4())
    request = {"type": "get_stack_outputs", "id": request_id}
    click.echo(f"Requesting stack outputs: {request}")
    resource_request_queue.put(request)

    # Wait for response with matching ID
    start_time = time.time()
    timeout = 30  # Total timeout in seconds
    retry_interval = 5  # Time between retries in seconds

    while time.time() - start_time < timeout:
        try:
            click.echo("Waiting for table bucket response...")
            response = resource_response_queue.get(timeout=retry_interval)
            click.echo(f"Got response: {response}")
            if response["id"] == request_id:
                click.echo("Found matching response!")
                try:
                    return StackOutputs.model_validate_json(response["stack_outputs"])
                except Exception as e:
                    click.echo(f"Error parsing stack outputs: {str(e)}", err=True)
                    raise
            else:
                click.echo(
                    f"Response ID {response['id']} didn't match request ID {request_id}"
                )
        except queue.Empty:
            click.echo("No response received, retrying...")
            continue

    raise Exception(f"Timeout waiting for table bucket info after {timeout} seconds")
