import logging
from typing import Literal, Optional
from pyspark.sql import DataFrame, SparkSession

from nextdata.cli.types import SparkSchemaSpec
from nextdata.core.pulumi_context_manager import PulumiContextManager
from nextdata.util.s3_tables_utils import get_s3_table_path


class SparkManager:
    def __init__(
        self,
        bucket_arn: Optional[str] = None,
        namespace: Optional[str] = None,
    ):
        if not bucket_arn or not namespace:
            bucket_arn, namespace = PulumiContextManager.get_connection_info()
        self.bucket_arn = bucket_arn
        self.namespace = namespace
        self.spark = self.create_spark_session()

    def create_spark_session(self) -> SparkSession:
        """Create a SparkSession with AWS S3 and Iceberg configuration"""
        packages = [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
            "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
            "software.amazon.awssdk:bundle:2.21.1",
        ]

        return (
            SparkSession.builder.appName("NextData")
            # Iceberg catalog configuration
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
                self.bucket_arn,
            )
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            # Package dependencies
            .config("spark.jars.packages", ",".join(packages))
            # Create session
            .getOrCreate()
        )

    def test_connection(self) -> bool:
        """Test the connection to the SparkSession"""
        result = self.spark.sql("SELECT 1").collect()
        return len(result) > 0

    def create_table_from_df(
        self,
        table_name: str,
        df: DataFrame,
        schema: Optional[SparkSchemaSpec] = None,
        partition_keys: Optional[list[str]] = None,
    ) -> None:
        """Create a table"""
        table_path = get_s3_table_path(self.namespace, table_name)
        if schema:
            logging.error(
                f"Creating table {table_name} with schema {schema.model_dump_json()}"
            )
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_path} "
                f"({', '.join([f'{col} {dtype}' for col, dtype in schema.schema.items()])})"
                f"USING iceberg"
            )
        else:
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_path} "
                f"({', '.join([f'{col} {dtype}' for col, dtype in df.dtypes])})"
                f"USING iceberg"
            )
        if partition_keys:
            self.spark.sql(
                f"ALTER TABLE {table_path} SET PARTITION_BY ({', '.join(partition_keys)})"
            )

    def write_to_table(
        self,
        table_name: str,
        df: DataFrame,
        mode: Literal["overwrite", "append"] = "overwrite",
        schema: Optional[SparkSchemaSpec] = None,
    ) -> None:
        """Write data to a table"""
        logging.error(f"Writing to table {table_name} in namespace {self.namespace}")
        logging.error(f"Sample data:\n{df.limit(10).show()}")
        table_path = get_s3_table_path(self.namespace, table_name)
        self.create_table_from_df(table_name, df, schema)
        df.write.mode(mode).saveAsTable(table_path)

    def read_from_table(
        self, table_name: str, limit: int = 10, offset: int = 0
    ) -> DataFrame:
        """Read data from a table"""
        table_path = get_s3_table_path(self.namespace, table_name)
        if limit:
            return self.spark.sql(
                f"SELECT * FROM {table_path} LIMIT {limit} OFFSET {offset}"
            ).collect()
        return self.spark.sql(f"SELECT * FROM {table_path}").collect()

    def read_from_csv(self, file_path: str) -> DataFrame:
        """Read data from a CSV file"""
        return self.spark.read.csv(file_path, header=True, inferSchema=True)

    def get_table_metadata(self, table_name: str) -> dict:
        """Get table metadata"""
        table_path = get_s3_table_path(self.namespace, table_name)
        try:
            row_count = self.spark.sql(f"SELECT COUNT(*) FROM {table_path}").collect()[
                0
            ][0]
            schema = self.spark.sql(f"DESCRIBE {table_path}").collect()
            return {
                "row_count": row_count,
                "schema": schema,
            }
        except Exception as e:
            return {
                "row_count": -1,
                "schema": [],
            }

    def get_table(self, table_name: str) -> DataFrame:
        """Get a table"""
        table_path = get_s3_table_path(self.namespace, table_name)
        return self.spark.table(table_path)

    def delete_table(self, table_name: str) -> None:
        """Delete a table"""
        table_path = get_s3_table_path(self.namespace, table_name)
        logging.error(f"Deleting table {table_path}")
        self.spark.sql(f"DROP TABLE IF EXISTS {table_path} PURGE;")
