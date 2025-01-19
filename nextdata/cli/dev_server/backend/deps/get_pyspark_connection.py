import click

from nextdata.core.connections.spark import SparkManager


def pyspark_connection_dependency() -> SparkManager:
    """Get PySpark connection with S3 Tables configuration"""
    try:

        spark = SparkManager()
        return spark
    except Exception as e:
        click.echo(f"Error creating Spark session: {str(e)}", err=True)
        raise
