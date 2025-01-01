"""
Decorator for glue jobs. Handles some of the boilerplate for glue jobs.
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pydantic import BaseModel, ValidationError
from typing import Any, Callable, Literal, Optional, TypeVar
from functools import wraps

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

    job_name: str
    job_type: Literal["GLUE_ETL", "GLUE_RETL"]
    connection_name: str
    connection_type: SupportedConnectionTypes
    connection_properties: dict[str, Any]
    glue_db_name: str
    temp_dir: str
    sql_query: Optional[str] = None
    sql_table: str
    output_s3_path: str
    incremental_column: Optional[str] = None
    is_full_load: Optional[bool] = True


def glue_job(job_args_type: type[BaseModel] = GlueJobArgs):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def glue_job_wrapper(
            spark: SparkContext,
            glue_context: GlueContext,
            job: Job,
            job_args: job_args_type,
            *args,
            **kwargs,
        ) -> T:
            # Get standard Glue job arguments
            job_args = getResolvedOptions(
                sys.argv,
                [
                    "JOB_NAME",
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
                ],
            )
            try:
                job_args = job_args_type(**job_args)
            except ValidationError as e:
                print(f"Error validating job arguments: {e}")
                raise e

            # Set up Glue and Spark context
            glue_context = GlueContext(SparkContext.getOrCreate())
            spark = glue_context.spark_session
            job = Job(glue_context)
            job.init(job_args["JOB_NAME"], job_args)

            try:
                # Call the wrapped function with the initialized contexts
                result = func(
                    spark=spark,
                    glue_context=glue_context,
                    job=job,
                    job_args=job_args,
                    *args,
                    **kwargs,
                )
                job.commit()
                return result
            except Exception as e:
                # Log any errors and ensure job fails properly
                print(f"Error in Glue job: {str(e)}")
                raise e

        return glue_job_wrapper

    return decorator
