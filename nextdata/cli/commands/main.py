import importlib.resources
from pathlib import Path
import click
import dotenv

from nextdata.cli.commands import NDX_SINGLETON

from ..project_generator import NextDataGenerator
from .pulumi import pulumi
from .dev_server import dev_server

dotenv.load_dotenv(Path.cwd() / ".env")


@click.group()
def cli():
    """NextData (ndx) CLI"""
    pass


cli.add_command(pulumi)
cli.add_command(dev_server)


@cli.command(name="create-ndx-app")
@click.argument("app_name")
@click.option("--template", default="default", help="Template to use for the project")
def create_app(app_name: str, template: str):
    """Create a new NextData application"""
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
"""
        )
    except Exception as e:
        click.echo(f"Error creating project: {str(e)}", err=True)


@cli.command(name="dev")
@click.option("--skip-init", is_flag=True, help="Skip initialization of the stack")
def dev(skip_init: bool):
    """Start development server and watch for data changes"""
    NDX_SINGLETON.start(skip_init=skip_init)


@cli.command(name="list-templates")
def list_templates():
    """List available templates"""
    try:
        templates_path = importlib.resources.files("nextdata") / "templates"
        templates = [item.name for item in templates_path.iterdir() if item.is_dir()]

        if templates:
            click.echo("Available templates:")
            for template in templates:
                # Check if template has a description in its cookiecutter.json
                template_json = templates_path / template / "cookiecutter.json"
                description = "No description available"
                if template_json.exists():
                    import json

                    with open(template_json) as f:
                        try:
                            data = json.load(f)
                            description = data.get("description", description)
                        except json.JSONDecodeError:
                            pass

                click.echo(f"  - {template}: {description}")
        else:
            click.echo("No templates found")

    except Exception as e:
        click.echo(f"Error listing templates: {str(e)}", err=True)


def main():
    cli()


if __name__ == "__main__":
    main()
