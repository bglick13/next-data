from fastapi import Request


from nextdata.core.pulumi_context_manager import PulumiContextManager
from nextdata.cli.types import StackOutputs


def get_stack_outputs(request: Request) -> StackOutputs:
    """Get stack outputs from the main thread"""
    pulumi_context_manager: PulumiContextManager = (
        request.app.state.pulumi_context_manager
    )
    return pulumi_context_manager.get_stack_outputs()
