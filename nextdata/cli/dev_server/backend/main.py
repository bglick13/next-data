import tempfile
from typing import Annotated
from fastapi import FastAPI, Form
from pyspark.sql import SparkSession
from fastapi.middleware.cors import CORSMiddleware
import logging
from .deps.get_pyspark_connection import pyspark_connection_dependency
from nextdata.cli.types import StackOutputs, UploadCsvRequest
from pathlib import Path
from fastapi import Depends, File, UploadFile, Path as FastAPI_Path

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def health_check(
    spark_stack_outputs: Annotated[
        tuple[SparkSession, StackOutputs],
        Depends(pyspark_connection_dependency),
    ]
):
    spark, stack_outputs = spark_stack_outputs
    try:
        connection_check = spark.sql("SELECT 1").collect()
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
    spark_stack_outputs: Annotated[
        tuple[SparkSession, StackOutputs],
        Depends(pyspark_connection_dependency),
    ],
    file: UploadFile = File(...),
    table_name: str = Form(...),
):
    spark, stack_outputs = spark_stack_outputs
    data_dir = Path.cwd() / "data"
    valid_directories = [d.name for d in data_dir.iterdir() if d.is_dir()]
    table_name_is_valid = table_name in valid_directories
    logging.info(f"Table name {table_name} is valid: {table_name_is_valid}")
    if not table_name_is_valid:
        return {
            "status": "error",
            "error": f"Table name {table_name} is not a valid directory",
        }
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(file.file.read())
            temp_file_path = temp_file.name
            df = spark.read.csv(temp_file_path, header=True, inferSchema=True)
        values = df.toJSON().collect()
        logging.info(f"Values: {values}")
        sql_statement = f"insert into {stack_outputs.table_bucket['outputs']['name']}.{stack_outputs.table_namespace['outputs']['name']}.{table_name} values {values}"
        logging.info(f"SQL Statement: {sql_statement}")
        spark.sql(sql_statement)
        return {"status": "success", "filename": file.filename}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/table/{table_name}/metadata")
async def get_table_metadata(
    spark_stack_outputs: Annotated[
        tuple[SparkSession, StackOutputs],
        Depends(pyspark_connection_dependency),
    ],
    table_name: str = FastAPI_Path(...),
):
    spark, stack_outputs = spark_stack_outputs
    row_count = spark.sql(
        f"SELECT COUNT(*) FROM {stack_outputs.table_bucket['outputs']['name']}.{stack_outputs.table_namespace['outputs']['name']}.{table_name}"
    ).collect()[0][0]
    return {"status": "success", "row_count": row_count}
