"""
Decorator for glue jobs. Handles some of the boilerplate for glue jobs.
"""

import json

from pydantic import BaseModel, ConfigDict, field_validator
from typing import Any, Callable, Literal, Optional, TypeVar
from functools import wraps
import argparse
from nextdata.core.connections.spark import SparkManager

T = TypeVar("T")
SupportedConnectionTypes = Literal[
    "s3", "redshift", "snowflake", "athena", "jdbc", "dsql"
]


def add_model(parser: argparse.ArgumentParser, model: BaseModel):
    "Add Pydantic model to an ArgumentParser"
    fields = model.model_fields
    for name, field in fields.items():
        parser.add_argument(
            f"--{name}",
            dest=name,
            type=str,
            default=None if field.default is None else str(field.default),
            help=field.description,
        )


class GlueJobArgs(BaseModel):
    """
    Arguments for a glue job.

    Args:
        job_name: The name of the glue job.
        temp_dir: The temporary directory for the glue job.
        sql_table: The table to run the SQL query on.
        incremental_column: The column to use for incremental loading.
        is_full_load: Whether the job is a full load.
    """

    job_name: str
    connection_name: str
    connection_type: SupportedConnectionTypes
    connection_properties: dict[str, Any]
    sql_table: str
    incremental_column: Optional[str] = None
    is_full_load: Optional[bool] = True
    bucket_arn: str
    namespace: str

    model_config = ConfigDict(extra="allow")

    # ConnectionProperties comes in as a raw string, so we need to parse it in the validator
    @field_validator("connection_properties", mode="before")
    def validate_connection_properties(cls, v):
        if isinstance(v, dict):
            return v
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            raise ValueError("Invalid connection properties")

    @field_validator("is_full_load", mode="before")
    def validate_is_full_load(cls, v):
        if isinstance(v, bool):
            return v
        return v.lower() == "true"


def glue_job(JobArgsType: type[GlueJobArgs] = GlueJobArgs):
    def decorator(func: Callable[[SparkManager, JobArgsType], T]) -> Callable[..., T]:
        @wraps(func)
        def glue_job_wrapper(
            *args,
            **kwargs,
        ) -> T:
            parser = argparse.ArgumentParser()
            add_model(parser, JobArgsType)
            job_args = parser.parse_args()
            job_args_resolved = JobArgsType(**vars(job_args))

            spark_manager = SparkManager(
                bucket_arn=job_args_resolved.bucket_arn,
                namespace=job_args_resolved.namespace,
            )

            try:
                # Call the wrapped function with the initialized contexts
                result = func(
                    spark_manager=spark_manager,
                    job_args=job_args_resolved,
                )
                # TODO: If job is retl, add logic to write to db
                # TODO: Add tracking for retl output table names and cleanup old tables when new job is run
                return result
            except Exception as e:
                # Log any errors and ensure job fails properly
                print(f"Error in Glue job: {str(e)}")
                raise e

        return glue_job_wrapper

    return decorator
