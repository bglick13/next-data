"""
Decorator for glue jobs. Handles some of the boilerplate for glue jobs.
"""

import json
import sys
import os
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from typing import Any, Callable, Literal, Optional, TypeVar
from functools import wraps

from nextdata.core.connections.spark import SparkManager

T = TypeVar("T")
SupportedConnectionTypes = Literal[
    "s3", "redshift", "snowflake", "athena", "jdbc", "dsql"
]


class GlueJobArgs(BaseModel):
    """
    Arguments for a glue job.

    Args:
        job_name: The name of the glue job.
        temp_dir: The temporary directory for the glue job.
        sql_query: The SQL query to run.
        sql_table: The table to run the SQL query on.
        output_s3_path: The S3 path to write the output to.
        incremental_column: The column to use for incremental loading.
        is_full_load: Whether the job is a full load.
    """

    JOB_NAME: str
    JobType: Literal["GLUE_ETL", "GLUE_RETL"]
    ConnectionName: str
    ConnectionType: SupportedConnectionTypes
    ConnectionProperties: dict[str, Any]
    GlueDBName: str
    TempDir: str
    SQLQuery: Optional[str] = None
    SQLTable: str
    OutputS3Path: str
    IncrementalColumn: Optional[str] = None
    IsFullLoad: Optional[bool] = True
    bucket_arn: str
    namespace: str

    model_config = ConfigDict(extra="allow")

    # ConnectionProperties comes in as a raw string, so we need to parse it in the validator
    @field_validator("ConnectionProperties", mode="before")
    def validate_connection_properties(cls, v):
        return json.loads(v)

    @field_validator("IsFullLoad", mode="before")
    def validate_is_full_load(cls, v):
        return v.lower() == "true"


def glue_job(job_args_type: type[GlueJobArgs] = GlueJobArgs):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def glue_job_wrapper(
            *args,
            **kwargs,
        ) -> T:
            # Get standard Glue job arguments
            job_args_resolved = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
                    "JobType",
                    "ConnectionName",
                    "ConnectionType",
                    "ConnectionProperties",
                    "GlueDBName",
                    "TempDir",
                    "SQLQuery",
                    "SQLTable",
                    "OutputS3Path",
                    "IncrementalColumn",
                    "IsFullLoad",
                    # Config values
                    "project_name",
                    "project_slug",
                    "aws_region",
                    "project_dir",
                    "data_dir",
                    "connections_dir",
                    "stack_name",
                    "bucket_arn",
                    "namespace",
                ],
            )
            os.environ["PROJECT_NAME"] = job_args_resolved["project_name"]
            os.environ["PROJECT_SLUG"] = job_args_resolved["project_slug"]
            os.environ["AWS_REGION"] = job_args_resolved["aws_region"]
            os.environ["PROJECT_DIR"] = job_args_resolved["project_dir"]
            os.environ["DATA_DIR"] = job_args_resolved["data_dir"]
            os.environ["CONNECTIONS_DIR"] = job_args_resolved["connections_dir"]
            os.environ["STACK_NAME"] = job_args_resolved["stack_name"]
            try:
                job_args: job_args_type = job_args_type(**job_args_resolved)
            except ValidationError as e:
                print(f"Error validating job arguments: {e}")
                raise e

            spark_manager = SparkManager(
                bucket_arn=job_args.bucket_arn,
                namespace=job_args.namespace,
            )
            # Set up Glue and Spark context
            glue_context = GlueContext(spark_manager.spark.sparkContext)
            job = Job(glue_context)
            job.init(job_args.JOB_NAME, job_args_resolved)

            try:
                # Call the wrapped function with the initialized contexts
                result = func(
                    spark_manager=spark_manager,
                    glue_context=glue_context,
                    job=job,
                    job_args=job_args,
                )
                job.commit()
                return result
            except Exception as e:
                # Log any errors and ensure job fails properly
                print(f"Error in Glue job: {str(e)}")
                raise e

        return glue_job_wrapper

    return decorator
