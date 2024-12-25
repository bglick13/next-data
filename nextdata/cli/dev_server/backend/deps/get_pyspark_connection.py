from typing import Annotated

from fastapi import Depends
from pyspark.sql import SparkSession
import click
from nextdata.cli.dev_server.backend.deps.get_stack_outputs import get_stack_outputs
from nextdata.cli.types import StackOutputs


def pyspark_connection_dependency(
    stack_outputs: Annotated[StackOutputs, Depends(get_stack_outputs)]
) -> tuple[SparkSession, StackOutputs]:
    """Get PySpark connection with S3 Tables configuration"""
    try:
        # Get bucket info from main thread
        click.echo(
            f"Connecting to S3 Tables bucket: {stack_outputs.table_bucket['outputs']['arn']}"
        )

        spark = (
            SparkSession.builder.appName("NextData")
            .config(
                "spark.sql.catalog.s3tablesbucket",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(
                "spark.sql.catalog.s3tablesbucket.catalog-impl",
                "software.amazon.s3tables.iceberg.S3TablesCatalog",
            )
            .config(
                "spark.sql.catalog.s3tablesbucket.warehouse",
                stack_outputs.table_bucket["outputs"]["arn"],
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                "spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
            )
            .getOrCreate()
        )
        return spark, stack_outputs
    except Exception as e:
        click.echo(f"Error creating Spark session: {str(e)}", err=True)
        raise
