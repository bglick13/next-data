import json
import click

from nextdata.cli.commands import NDX_SINGLETON


@click.group()
def pulumi():
    """Pulumi commands"""
    pass


@pulumi.command(name="up")
def up():
    """Pulumi up"""
    NDX_SINGLETON.pulumi_context_manager.create_stack()


@pulumi.command(name="preview")
def preview():
    """Pulumi preview"""
    NDX_SINGLETON.pulumi_context_manager.preview_stack()


@pulumi.command(name="refresh")
def refresh():
    """Pulumi refresh"""
    NDX_SINGLETON.pulumi_context_manager.refresh_stack()


@pulumi.command(name="destroy")
def destroy():
    """Pulumi destroy"""
    NDX_SINGLETON.pulumi_context_manager.destroy_stack()


@pulumi.command(name="outputs")
def outputs():
    """Pulumi outputs"""
    response = NDX_SINGLETON.pulumi_context_manager.stack.export_stack()
    click.echo(json.dumps(response.deployment, indent=2))
