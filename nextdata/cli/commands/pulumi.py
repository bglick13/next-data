import click

from nextdata.cli.ndx_context_manager import NdxContextManager


@click.group()
def pulumi():
    """Pulumi commands"""
    pass


@pulumi.command(name="up")
def up():
    """Pulumi up"""
    ndx_context_manager = NdxContextManager()
    ndx_context_manager.pulumi_context_manager.create_stack()


@pulumi.command(name="preview")
def preview():
    """Pulumi preview"""
    ndx_context_manager = NdxContextManager()
    ndx_context_manager.pulumi_context_manager.preview_stack()


@pulumi.command(name="refresh")
def refresh():
    """Pulumi refresh"""
    ndx_context_manager = NdxContextManager()
    ndx_context_manager.pulumi_context_manager.refresh_stack()


@pulumi.command(name="destroy")
def destroy():
    """Pulumi destroy"""
    ndx_context_manager = NdxContextManager()
    ndx_context_manager.pulumi_context_manager.destroy_stack()
