from pydantic import BaseModel


class StackOutputs(BaseModel):
    project_name: str
    stack_name: str
    resources: list[dict]
    table_bucket: dict
    table_namespace: dict
    tables: list[dict]


class UploadCsvRequest(BaseModel):
    table_name: str
    mode: str = "append"
