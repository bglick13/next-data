import json

import asyncclick as click

from nextdata.core.pulumi_context_manager import PulumiContextManager


@click.group()
def pulumi() -> None:
    """Pulumi commands."""


@pulumi.command(name="up")
def up() -> None:
    """Pulumi up."""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.create_stack()


@pulumi.command(name="cancel")
def cancel() -> None:
    """Pulumi cancel."""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.cancel_update()


@pulumi.command(name="preview")
def preview() -> None:
    """Pulumi preview."""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.preview_stack()


@pulumi.command(name="refresh")
def refresh() -> None:
    """Pulumi refresh."""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.refresh_stack()


@pulumi.command(name="destroy")
def destroy() -> None:
    """Pulumi destroy."""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.refresh_stack()
    pulumi_context_manager.destroy_stack()


@pulumi.command(name="outputs")
def outputs() -> None:
    """Pulumi outputs."""
    pulumi_context_manager = PulumiContextManager()
    response = pulumi_context_manager.stack.export_stack()
    click.echo(json.dumps(response.deployment, indent=2))
