import tempfile
from typing import Annotated
from fastapi import FastAPI, Form, Query
from fastapi.middleware.cors import CORSMiddleware
import logging

from nextdata.cli.dev_server.backend.deps.get_stack_outputs import get_stack_outputs
from nextdata.core.pulumi_context_manager import PulumiContextManager
from nextdata.core.connections.spark import SparkManager

from .deps.get_pyspark_connection import pyspark_connection_dependency
from nextdata.cli.types import Checker, StackOutputs, UploadCsvRequest
from pathlib import Path
from fastapi import Depends, File, UploadFile, Path as FastAPI_Path
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.initialize_stack()
    app.state.pulumi_context_manager = pulumi_context_manager
    yield
    app.state.clear()


app_state = {}
app = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health_check(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    stack_outputs: Annotated[StackOutputs, Depends(get_stack_outputs)],
):
    try:
        connection_check = spark.test_connection()
        return {
            "status": "healthy" if connection_check else "unhealthy",
            "pulumi_stack": stack_outputs.stack_name,
            "stack_outputs": stack_outputs,
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "pulumi_stack": stack_outputs.stack_name,
            "stack_outputs": stack_outputs.model_dump_json(),
        }


@app.get("/api/data_directories")
async def list_data_directories():
    data_dir = Path.cwd() / "data"
    if not data_dir.exists():
        return {"directories": []}

    directories = [
        {
            "name": d.name,
            "path": str(d.relative_to(data_dir)),
            "type": "directory" if d.is_dir() else "file",
        }
        for d in data_dir.iterdir()
        if d.is_dir()
    ]
    return {"directories": directories}


@app.post("/api/upload_csv")
async def upload_csv(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    file: UploadFile = File(...),
    form_data: UploadCsvRequest = Depends(Checker(UploadCsvRequest)),
):
    data_dir = Path.cwd() / "data"
    valid_directories = [d.name for d in data_dir.iterdir() if d.is_dir()]
    table_name_is_valid = form_data.table_name in valid_directories
    logging.info(f"Table name {form_data.table_name} is valid: {table_name_is_valid}")
    if not table_name_is_valid:
        return {
            "status": "error",
            "error": f"Table name {form_data.table_name} is not a valid directory",
        }
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(file.file.read())
            temp_file_path = temp_file.name
            df = spark.read_from_csv(temp_file_path)
        logging.error(form_data.model_dump_json())
        spark.write_to_table(
            form_data.table_name,
            df,
            schema=form_data.schema,
        )
        return {"status": "success", "filename": file.filename}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/table/{table_name}/metadata")
async def get_table_metadata(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    table_name: str = FastAPI_Path(...),
):
    return spark.get_table_metadata(table_name)


@app.get("/api/table/{table_name}/data")
async def get_sample_data(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    table_name: str = FastAPI_Path(...),
    limit: int = Query(10),
    offset: int = Query(0),
):
    return spark.read_from_table(
        table_name,
        limit,
        offset,
    )
