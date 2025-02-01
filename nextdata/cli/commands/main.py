import importlib.resources
from pathlib import Path

import asyncclick as click
import dotenv

from nextdata.cli.dashboard_installer import DashboardInstaller
from nextdata.cli.dev_server.main import DevServer
from nextdata.cli.project_generator import NextDataGenerator

from .dev_server import dev_server
from .pulumi import pulumi
from .spark import spark

dotenv.load_dotenv(Path.cwd() / ".env")


@click.group()
def cli() -> None:
    """NextData (ndx) CLI."""


cli.add_command(pulumi)
cli.add_command(dev_server)
cli.add_command(spark)


@cli.command(name="create-ndx-app")
@click.argument("app_name")
@click.option("--template", default="default", help="Template to use for the project")
def create_app(app_name: str, template: str) -> None:
    """Create a new NextData application."""
    try:
        generator = NextDataGenerator(app_name, template)
        generator.create_project()
        click.echo(
            f"""
✨ Created NextData app: {app_name}

To get started:
  cd {app_name}
  pip install -r requirements.txt
  ndx dev
""",
        )
    except ValueError as e:
        click.echo(f"Error creating project: {e!s}", err=True)


@cli.command(name="dev")
@click.option("--skip-init", is_flag=True, help="Skip initialization of the stack")
@click.option("--dashboard-port", type=int, default=3000, help="Port to run the dashboard on")
@click.option("--api-port", type=int, default=8000, help="Port to run the API on")
async def dev(skip_init: bool, dashboard_port: int, api_port: int) -> None:  # noqa: FBT001
    """Start development server and watch for data changes."""
    dashboard_installer = DashboardInstaller()
    dashboard_installer.install()
    dev_server = DevServer()
    await dev_server.start_async(
        skip_init=skip_init,
        dashboard_port=dashboard_port,
        api_port=api_port,
    )


@cli.command(name="list-templates")
def list_templates() -> None:
    """List available templates."""
    try:
        templates_path = importlib.resources.files("nextdata") / "templates"
        templates = [item.name for item in templates_path.iterdir() if item.is_dir()]

        if templates:
            click.echo("Available templates:")
            for template in templates:
                # Check if template has a description in its cookiecutter.json
                template_json = templates_path / template / "cookiecutter.json"
                description = "No description available"
                if template_json.is_file():
                    import json

                    with Path(template_json.name).open() as f:
                        try:
                            data = json.load(f)
                            description = data.get("description", description)
                        except json.JSONDecodeError:
                            pass

                click.echo(f"  - {template}: {description}")
        else:
            click.echo("No templates found")

    except ValueError as e:
        click.echo(f"Error listing templates: {e!s}", err=True)


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
