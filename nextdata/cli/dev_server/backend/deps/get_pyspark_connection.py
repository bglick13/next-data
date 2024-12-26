from typing import Annotated

from fastapi import Depends
from pyspark.sql import SparkSession
import click
from nextdata.cli.dev_server.backend.deps.get_stack_outputs import get_stack_outputs
from nextdata.core.connections.spark import SparkManager
from nextdata.cli.types import StackOutputs


def pyspark_connection_dependency() -> SparkManager:
    """Get PySpark connection with S3 Tables configuration"""
    try:

        spark = SparkManager()
        return spark
    except Exception as e:
        click.echo(f"Error creating Spark session: {str(e)}", err=True)
        raise
