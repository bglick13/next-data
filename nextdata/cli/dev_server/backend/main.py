import asyncio
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Annotated, Any

import boto3
from fastapi import Depends, FastAPI, File, Form, Query, UploadFile
from fastapi import Path as FastAPI_Path
from fastapi.middleware.cors import CORSMiddleware

from nextdata.cli.dev_server.backend.deps.get_db import get_db_dependency
from nextdata.cli.types import Checker, UploadCsvRequest
from nextdata.core.connections.spark import SparkManager
from nextdata.core.db.db_manager import DatabaseManager
from nextdata.core.db.models import HumanReadableName
from nextdata.core.glue.glue_entrypoint import GlueJobArgs

from .deps.get_pyspark_connection import pyspark_connection_dependency

app_state = {}
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
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
) -> dict[str, str]:
    try:
        connection_check = spark.test_connection()
        return {
            "status": "healthy" if connection_check else "unhealthy",
        }
    except ValueError as e:
        return {
            "status": "error",
            "error": str(e),
        }


@app.get("/api/data_directories")
async def list_data_directories() -> dict[str, list[dict[str, str]]]:
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


@app.get("/api/table/{table_name}/jobs")
async def get_table_jobs(
    db_manager: Annotated[DatabaseManager, Depends(get_db_dependency)],
    table_name: Annotated[str, FastAPI_Path(...)],
) -> list[dict[str, Any]]:
    logging.error(f"Fetching jobs for table: {table_name}")
    table = db_manager.get_table_by_name(table_name)
    logging.error(f"Table: {table.__dict__}")
    if not table:
        logging.error(f"Table not found: {table_name}")
        return []

    # Get all jobs that have this table as input or output
    input_jobs = list(table.downstream_jobs)
    output_jobs = list(table.upstream_jobs)

    logging.error(f"Found {len(input_jobs)} input jobs and {len(output_jobs)} output jobs")
    logging.error(f"Input jobs: {[job.name for job in input_jobs]}")
    logging.error(f"Output jobs: {[job.name for job in output_jobs]}")

    # Combine and deduplicate jobs
    all_jobs = {job.id: job for job in input_jobs + output_jobs}

    # Convert to list of dicts for JSON response
    return [
        {
            "id": job.id,
            "name": job.name,
            "jobType": job.job_type.value,
            "connectionName": job.connection_name,
            "connectionType": (job.connection_type.value if job.connection_type else None),
            "sqlTable": job.sql_table,
            "incrementalColumn": job.incremental_column,
            "isFullLoad": job.is_full_load,
        }
        for job in all_jobs.values()
    ]


@app.post("/api/upload_csv")
async def upload_csv(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    file: Annotated[UploadFile, File(...)],
    form_data: Annotated[UploadCsvRequest, Depends(Checker(UploadCsvRequest))],
) -> dict[str, Any]:
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
            temp_df = spark.read_from_csv(temp_file_path)
        logging.error(form_data.model_dump_json())
        spark.write_to_table(
            form_data.table_name,
            temp_df,
            schema=form_data.schema,
        )
        return {"status": "success", "filename": file.filename}
    except ValueError as e:
        return {"status": "error", "error": str(e)}


@app.get("/api/table/{table_name}/metadata")
async def get_table_metadata(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    table_name: Annotated[str, FastAPI_Path(...)],
) -> dict[str, Any]:
    return spark.get_table_metadata(table_name)


@app.get("/api/table/{table_name}/data")
async def get_sample_data(
    spark: Annotated[SparkManager, Depends(pyspark_connection_dependency)],
    table_name: Annotated[str, FastAPI_Path(...)],
    limit: Annotated[int, Query()] = 10,
    offset: Annotated[int, Query()] = 0,
) -> list[dict]:
    return (
        spark.read_from_table(
            table_name,
            limit,
            offset,
        )
        .toPandas()
        .to_dict(orient="records")
    )


@app.post("/api/jobs/trigger")
async def trigger_job(
    db_manager: Annotated[DatabaseManager, Depends(get_db_dependency)],
    job_name: Annotated[str, Form(...)],
) -> dict[str, Any]:
    sts_client = boto3.client("sts")
    job = db_manager.get_job(job_name)
    logging.error(f"Running Job: {job.__dict__}")
    glue_role = db_manager.get_resource_by_name(HumanReadableName.GLUE_ROLE)
    emr_app = db_manager.get_resource_by_name(HumanReadableName.EMR_APP)
    s3_bucket = db_manager.get_resource_by_name(HumanReadableName.S3_TABLE_BUCKET)
    s3_namespace = db_manager.get_resource_by_name(HumanReadableName.S3_TABLE_NAMESPACE)
    if not glue_role or not emr_app or not s3_bucket or not s3_namespace or not job:
        raise ValueError
    glue_role_arn = glue_role.resource_arn
    emr_app_id = emr_app.resource_arn
    emr_app_id = emr_app_id.split("/")[-1]
    assumed_role = sts_client.assume_role(
        RoleArn=glue_role_arn,
        RoleSessionName="dashboard-job-trigger",
    )
    s3_bucket_arn = s3_bucket.resource_arn
    s3_bucket_namespace = s3_namespace.name

    # Create EMR client with the assumed role credentials
    emr_client = boto3.client(
        "emr-serverless",
        aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed_role["Credentials"]["SessionToken"],
    )

    app_ready = False
    sent_start_request = False
    logging.error(f"App ID: {emr_app_id}")
    timeout = time.time() + 30
    while not app_ready:
        emr_app_state = emr_client.get_application(applicationId=emr_app_id)
        logging.error(f"App State: {emr_app_state}")
        if (
            emr_app_state["application"]["state"] == "CREATED"
            or emr_app_state["application"]["state"] == "STARTED"
        ):
            app_ready = True
        else:
            if not sent_start_request:
                emr_client.start_application(applicationId=emr_app_id)
                sent_start_request = True
            await asyncio.sleep(1)
        if time.time() > timeout:
            raise ValueError
    logging.error(f"Connection Properties:\n{job.connection_properties}")
    args = GlueJobArgs(
        job_name=job.name,
        job_type=job.job_type.value,
        connection_name=job.connection_name or "",
        connection_type=job.connection_type.value,  # type: ignore this is a valid value
        connection_properties=job.connection_properties or {},
        sql_table=job.sql_table or "",
        incremental_column=job.incremental_column or "",
        is_full_load=job.is_full_load or False,
        bucket_arn=s3_bucket_arn,
        namespace=s3_bucket_namespace,
    )
    args_list = []
    for name, value in args.model_dump().items():
        if value is None:
            continue
        if isinstance(value, dict):
            fixed_value = json.dumps(value)
        elif isinstance(value, bool):
            fixed_value = str(value).lower()  # Convert True/False to 'true'/'false'
        elif isinstance(value, str) and " " in value:
            fixed_value = f"'{value}'"  # Wrap strings containing spaces in quotes
        else:
            fixed_value = str(value)
        args_list.append(f"--{name}")  # Add argument name separately
        args_list.append(str(fixed_value))  # Add value separately
    logging.error(f"Args List:\n{args_list}")
    logging.error(f"Job: {job.__dict__}")
    packages = [
        "org.postgresql:postgresql:42.6.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
        "software.amazon.awssdk:bundle:2.21.1",
    ]
    response = emr_client.start_job_run(
        applicationId=emr_app_id,
        executionRoleArn=glue_role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{job.script.bucket}/{job.script.s3_path}",
                "entryPointArguments": args_list,
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=1 "
                    "--conf spark.executor.memory=4G "
                    "--conf spark.executor.instances=1 "
                    "--conf spark.driver.cores=1 "
                    "--conf spark.driver.memory=4G "
                    # Add dependencies using Maven coordinates
                    f"--conf spark.jars.packages={','.join(packages)} "
                    # Add iceberg and s3 extensions
                    "--conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog "  # noqa: E501
                    f"--conf spark.sql.catalog.s3tablesbucket.warehouse={args.bucket_arn} "
                    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "  # noqa: E501
                    # Add environment
                    f"--conf spark.archives=s3://{job.script.bucket}/{job.venv_s3_path}#environment "  # noqa: E501
                    f"--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python "  # noqa: E501
                    f"--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python "  # noqa: E501
                    "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python "
                ),
            },
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{job.script.bucket}/logs/"},
            },
        },
    )
    return {"jobRunId": response["jobRunId"], "applicationId": emr_app_id}


@app.get("/api/jobs/{application_id}/{job_run_id}/status")
async def get_job_status(
    db_manager: Annotated[DatabaseManager, Depends(get_db_dependency)],
    application_id: str,
    job_run_id: str,
) -> dict[str, Any]:
    """Get the status of a job run."""
    sts_client = boto3.client("sts")
    glue_role = db_manager.get_resource_by_name(HumanReadableName.GLUE_ROLE)
    if not glue_role:
        raise ValueError
    glue_role_arn = glue_role.resource_arn

    assumed_role = sts_client.assume_role(
        RoleArn=glue_role_arn,
        RoleSessionName="dashboard-job-status",
    )

    emr_client = boto3.client(
        "emr-serverless",
        aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed_role["Credentials"]["SessionToken"],
    )

    try:
        response = emr_client.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id,
        )
        return {
            "status": response["jobRun"]["state"],
            "stateDetails": response["jobRun"].get("stateDetails", ""),
            "failureReason": response["jobRun"].get("failureReason", ""),
            "startTime": response["jobRun"].get("startTime", ""),
            "endTime": response["jobRun"].get("endTime", ""),
        }
    except ValueError as e:
        return {"error": str(e)}


@app.get("/api/jobs/{application_id}/{job_run_id}/logs")
async def get_job_logs(
    db_manager: Annotated[DatabaseManager, Depends(get_db_dependency)],
    application_id: str,
    job_run_id: str,
) -> dict[str, Any]:
    """Get the logs for a job run."""
    sts_client = boto3.client("sts")
    glue_role = db_manager.get_resource_by_name(HumanReadableName.GLUE_ROLE)
    if not glue_role:
        raise ValueError
    glue_role_arn = glue_role.resource_arn

    assumed_role = sts_client.assume_role(
        RoleArn=glue_role_arn,
        RoleSessionName="dashboard-job-logs",
    )

    emr_client = boto3.client(
        "emr-serverless",
        aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed_role["Credentials"]["SessionToken"],
    )

    try:
        response = emr_client.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id,
        )

        # Get the CloudWatch logs
        logs_client = boto3.client(
            "logs",
            aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
            aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
            aws_session_token=assumed_role["Credentials"]["SessionToken"],
        )

        log_groups = [
            f"/aws-emr-serverless-logs/{application_id}/{job_run_id}/spark-job-driver",
            f"/aws-emr-serverless-logs/{application_id}/{job_run_id}/spark-job-executor",
        ]

        all_logs = []
        for log_group in log_groups:
            log_streams = logs_client.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            )

            if log_streams.get("logStreams"):
                for stream in log_streams["logStreams"]:
                    logs = logs_client.get_log_events(
                        logGroupName=log_group,
                        logStreamName=stream["logStreamName"],
                        startFromHead=True,
                    )

                    all_logs.extend(
                        {
                            "timestamp": event["timestamp"],
                            "message": event["message"],
                            "type": ("driver" if "driver" in log_group else "executor"),
                        }
                        for event in logs["events"]
                    )

        # Sort logs by timestamp
        all_logs.sort(key=lambda x: x["timestamp"])

        return {"status": response["jobRun"]["state"], "logs": all_logs}
    except ValueError as e:
        return {"error": str(e)}
