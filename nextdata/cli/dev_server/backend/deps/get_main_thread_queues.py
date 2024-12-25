from typing import Tuple
from queue import Queue
from fastapi import Request


async def get_main_thread_queues(request: Request) -> Tuple[Queue, Queue]:
    """Get main thread queues from FastAPI app state"""
    app = request.app
    if not hasattr(app.state, "resource_request_queue") or not hasattr(
        app.state, "resource_response_queue"
    ):
        raise RuntimeError("Resource queues not initialized in FastAPI app state")
    return app.state.resource_request_queue, app.state.resource_response_queue
