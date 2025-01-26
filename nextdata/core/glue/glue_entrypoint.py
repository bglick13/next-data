"""Decorator for glue jobs. Handles some of the boilerplate for glue jobs."""

import argparse
import json
from functools import wraps
from typing import Any, Callable, Literal, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator
from pyspark.sql import DataFrame
from sqlalchemy import URL

from nextdata.core.connections.spark import SparkManager
from nextdata.core.glue.connections.dsql import DSQLGlueJobArgs, generate_dsql_password
from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs, RemoteDBConnection
from nextdata.core.glue.connections.retl_connection import RetlDbConnection

T = TypeVar("T")
SupportedConnectionTypes = Literal[
    "s3",
    "redshift",
    "snowflake",
    "athena",
    "jdbc",
    "dsql",
]


def add_model(parser: argparse.ArgumentParser, model: BaseModel):
    """Add Pydantic model to an ArgumentParser"""
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
    job_type: Literal["etl", "retl"]
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


ConnectionClassType = TypeVar("ConnectionClassType", bound=RemoteDBConnection)


def get_db_connection_from_args(
    job_args: GlueJobArgs,
    connection_class: type[ConnectionClassType],
) -> ConnectionClassType:
    connect_args = {}
    url = None
    if job_args.connection_type == "dsql":
        connection_args: dict[str, Any] = job_args.connection_properties
        connection_conf = DSQLGlueJobArgs(host=connection_args["host"])
        password = generate_dsql_password(connection_conf.host)
        connect_args["sslmode"] = "require"
        connect_args["connect_timeout"] = 60
        url = URL.create(
            "postgresql",
            username="admin",
            password=password,
            host=connection_conf.host,
            database="postgres",
        )
    elif job_args.connection_type == "jdbc":
        connection_conf = JDBCGlueJobArgs(**job_args.connection_properties)
        password = connection_conf.password
        url = f"{connection_conf.protocol}://{connection_conf.username}:{password}@{connection_conf.host}:{connection_conf.port}/{connection_conf.database}"

    else:
        raise ValueError(f"Unsupported connection type: {job_args.connection_type}")
    return connection_class(url, connect_args, **job_args.model_dump())


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
                if job_args_resolved.job_type == "retl" and isinstance(
                    result,
                    DataFrame,
                ):
                    remote_db_connection = get_db_connection_from_args(
                        job_args_resolved,
                        RetlDbConnection,
                    )
                    remote_db_connection.write_retl_result(result)
                return result
            except Exception as e:
                # Log any errors and ensure job fails properly
                print(f"Error in Glue job: {e!s}")
                raise e

        return glue_job_wrapper

    return decorator
