from typing import Literal, Optional
from fastapi import Form, HTTPException
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ValidationError


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


class Checker:
    def __init__(self, model: BaseModel):
        self.model = model

    def __call__(self, data: str = Form(...)):
        try:
            return self.model.model_validate_json(data)
        except ValidationError as e:
            raise HTTPException(
                detail=jsonable_encoder(e.errors()),
                status_code=422,
            )
