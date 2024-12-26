import os
from pathlib import Path
from pydantic import BaseModel, Field
import dotenv

dotenv.load_dotenv(Path.cwd() / ".env")


class NextDataConfig(BaseModel):
    project_name: str = Field(default="default")
    project_slug: str = Field(default="default")
    aws_region: str = Field(default="us-east-1")
    aws_access_key_id: str = Field(default=None)
    aws_secret_access_key: str = Field(default=None)
    project_dir: Path = Field(default_factory=lambda: Path.cwd())
    data_dir: Path = Field(default_factory=lambda: Path.cwd() / "data")
    stack_name: str = Field(default="dev")

    @classmethod
    def from_env(cls):
        return cls(
            project_name=os.getenv("PROJECT_NAME"),
            project_slug=os.getenv("PROJECT_SLUG"),
            aws_region=os.getenv("AWS_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            stack_name=os.getenv("STACK_NAME", "dev"),
        )
