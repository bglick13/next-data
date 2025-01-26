from __future__ import annotations

import os
from pathlib import Path

import asyncclick as click
import dotenv
from pydantic import BaseModel, Field, ValidationError

dotenv.load_dotenv(Path.cwd() / ".env")


class NextDataConfig(BaseModel):
    project_name: str = Field(default="default")
    project_slug: str = Field(default="default")
    aws_region: str = Field(default="us-east-1")
    aws_access_key_id: str | None = Field(default=None)
    aws_secret_access_key: str | None = Field(default=None)
    project_dir: Path = Field(default_factory=lambda: Path.cwd())
    data_dir: Path = Field(default_factory=lambda: Path.cwd() / "data")
    connections_dir: Path = Field(default_factory=lambda: Path.cwd() / "connections")
    stack_name: str = Field(default="dev")

    @classmethod
    def from_env(cls) -> NextDataConfig | None:
        try:
            return cls(
                project_name=os.getenv("PROJECT_NAME") or "",
                project_slug=os.getenv("PROJECT_SLUG") or "",
                aws_region=os.getenv("AWS_REGION") or "us-east-1",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or None,
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or None,
                stack_name=os.getenv("STACK_NAME", "dev") or "dev",
            )
        except ValidationError as e:
            click.echo(
                "Could not initialize project config. "
                "Make sure you have a .env file in the root of your project, "
                "or some commands may not work as expected.",
            )
            click.echo(f"Error loading environment variables: {e}")

    def get_available_connections(self) -> list[str]:
        return [f.name for f in self.connections_dir.iterdir() if f.is_dir()]

    def get_available_tables(self) -> set[str]:
        return {f.name for f in self.data_dir.iterdir() if f.is_dir()}
