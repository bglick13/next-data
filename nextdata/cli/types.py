from typing import Generic, Literal, Optional, TypeVar

from fastapi import Form
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class StackOutputs(BaseModel):
    project_name: str
    stack_name: str
    resources: list[dict]
    table_bucket: dict
    table_namespace: dict
    tables: list[dict]
    glue_role: dict
    emr_app: dict
    emr_script_bucket: dict
    emr_scripts: list[dict]
    emr_jobs: list[dict]


class SparkSchemaSpec(BaseModel):
    schema: dict[
        str,
        Literal[
            "STRING",
            "DOUBLE",
            "INT",
            "FLOAT",
            "BOOLEAN",
            "TIMESTAMP",
            "DATE",
            "LONG",
        ],
    ]


class UploadCsvRequest(BaseModel):
    table_name: str
    mode: str = "append"
    schema: Optional[SparkSchemaSpec] = None


class Checker(Generic[T]):
    def __init__(self, model: type[T]) -> None:
        self.model = model

    def __call__(self, data: str = Form(...)) -> T:
        return self.model.model_validate_json(data)
