import os
import shutil
from pathlib import Path
import click
import importlib.resources
from cookiecutter.main import cookiecutter
from cookiecutter.generate import generate_context
from cookiecutter.utils import work_in


class NextDataGenerator:
    def __init__(self, app_name: str, template: str = "default"):
        self.app_name = app_name
        self.final_app_name = app_name.split("/")[-1]
        self.template = template
        self.app_dir = Path.cwd() / app_name

        # Get the absolute path to the template directory
        try:
            click.echo(f"Using template: {template}")

            # Use importlib to find the template directory
            templates_path = importlib.resources.files("nextdata") / "templates"
            template_path = templates_path / template
            click.echo(f"Looking for template at: {template_path}")

            if not template_path.exists():
                click.echo("Warning: Template path does not exist!")
                # List available templates
                click.echo("Available templates:")
                try:
                    for item in templates_path.iterdir():
                        if item.is_dir():
                            click.echo(f"  - {item.name}")
                except Exception as e:
                    click.echo(f"  Error listing templates: {str(e)}")

            self.template_dir = template_path

            # Verify cookiecutter.json exists
            cookiecutter_json = self.template_dir / "cookiecutter.json"
            if not cookiecutter_json.exists():
                click.echo(
                    f"Warning: cookiecutter.json not found at {cookiecutter_json}"
                )

        except Exception as e:
            click.echo(f"Error finding template: {str(e)}")
            raise click.ClickException("Failed to initialize project generator")

    def create_project(self):
        """Create a new NextData project"""
        click.echo(f"Creating NextData project: {self.app_name}")
        if self.app_dir.exists():
            raise click.ClickException(f"Directory {self.app_name} already exists")

        # Create context for cookiecutter
        context = {
            "project_name": self.final_app_name,
            "project_slug": self.final_app_name.lower()
            .replace(" ", "")
            .replace("-", "")
            .replace("_", ""),
        }

        click.echo(f"Using template directory: {self.template_dir}")
        click.echo(f"Context: {context}")

        # Generate project using cookiecutter
        try:
            with work_in(str(Path.cwd())):
                cookiecutter(
                    str(self.template_dir),
                    no_input=True,
                    extra_context=context,
                    output_dir=".",
                )
            # Add .env file to .gitignore
            with open(self.app_dir / ".gitignore", "a") as f:
                f.write("\n.env\n")
        except Exception as e:
            click.echo(f"Error generating project: {str(e)}")
            raise click.ClickException("Failed to generate project")

        # Initialize git repository
        self._initialize_git()

    def _initialize_git(self):
        """Initialize git repository"""
        os.system(f"cd {self.app_dir} && git init")
