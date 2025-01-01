import json
import asyncclick as click

from nextdata.core.pulumi_context_manager import PulumiContextManager


@click.group()
def pulumi():
    """Pulumi commands"""
    pass


@pulumi.command(name="up")
def up():
    """Pulumi up"""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.create_stack()


@pulumi.command(name="preview")
def preview():
    """Pulumi preview"""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.preview_stack()


@pulumi.command(name="refresh")
def refresh():
    """Pulumi refresh"""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.refresh_stack()


@pulumi.command(name="destroy")
def destroy():
    """Pulumi destroy"""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.refresh_stack()
    pulumi_context_manager.destroy_stack()


@pulumi.command(name="outputs")
def outputs():
    """Pulumi outputs"""
    pulumi_context_manager = PulumiContextManager()
    response = pulumi_context_manager.stack.export_stack()
    click.echo(json.dumps(response.deployment, indent=2))
