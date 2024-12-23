import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    TimestampType,
    LongType,
)
import json
from typing import Optional, Dict, Any
from datetime import datetime


class S3TablesManager:
    def __init__(self, region: str, bucket_name: str):
        self.region = region
        self.bucket_name = bucket_name
        self.s3tables_client = boto3.client("s3control")
        self.glue_client = boto3.client("glue")

        # Initialize iceberg catalog
        self.catalog = load_catalog(
            "glue",
            **{
                "warehouse": f"s3://tables/{bucket_name}",
                "region": region,
            },
        )

    def ensure_table_bucket_exists(self) -> bool:
        """Create table bucket if it doesn't exist"""
        try:
            response = self.s3tables_client.create_table_bucket(
                Bucket=self.bucket_name, Region=self.region
            )
            print(f"Created table bucket: {self.bucket_name}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyExists":
                return True
            raise e

    def create_table(self, table_name: str, schema: Schema) -> None:
        """Create a new Iceberg table"""
        try:
            self.catalog.create_table(
                identifier=f"{self.bucket_name}.{table_name}",
                schema=schema,
                location=f"s3://tables/{self.bucket_name}/{table_name}",
            )
            print(f"Created table: {table_name}")
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            raise e

    def infer_schema_from_data(self, data: pd.DataFrame) -> Schema:
        """Infer Iceberg schema from pandas DataFrame"""
        fields = []

        for column, dtype in data.dtypes.items():
            if pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = TimestampType()
            elif pd.api.types.is_integer_dtype(dtype):
                field_type = LongType()
            else:
                field_type = StringType()

            fields.append(
                NestedField.required(
                    field_id=len(fields) + 1, name=column, field_type=field_type
                )
            )

        return Schema(*fields)

    def sync_directory(self, directory_path: str, table_name: str) -> None:
        """Sync a local directory to an S3 table"""
        # Ensure table bucket exists
        self.ensure_table_bucket_exists()

        # Read all CSV files in directory
        dfs = []
        for file in directory_path.glob("*.csv"):
            df = pd.read_csv(file)
            dfs.append(df)

        if not dfs:
            print(f"No CSV files found in {directory_path}")
            return

        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)

        # Infer schema if table doesn't exist
        try:
            table = self.catalog.load_table(f"{self.bucket_name}.{table_name}")
        except Exception:
            schema = self.infer_schema_from_data(combined_df)
            self.create_table(table_name, schema)
            table = self.catalog.load_table(f"{self.bucket_name}.{table_name}")

        # Convert DataFrame to PyArrow table
        table = pa.Table.from_pandas(combined_df)

        # Write to S3 Tables using Iceberg
        with table.write_to(f"s3://tables/{self.bucket_name}/{table_name}") as writer:
            writer.write_table(table)

        print(f"Synced {len(combined_df)} rows to table: {table_name}")


class EnhancedDataDirectoryHandler(FileSystemEventHandler):
    def __init__(self, data_dir: Path, config: NextDataConfig):
        self.data_dir = data_dir
        self.config = config
        self.s3_manager = None

        if config.config["aws"]["table_bucket"]:
            self.s3_manager = S3TablesManager(
                region=config.config["aws"]["region"],
                bucket_name=config.config["aws"]["table_bucket"],
            )

    def on_created(self, event):
        if event.is_directory:
            print(f"📁 New directory created: {event.src_path}")
            self._handle_directory_change(Path(event.src_path))

    def on_modified(self, event):
        if event.is_directory:
            print(f"📝 Directory modified: {event.src_path}")
            self._handle_directory_change(Path(event.src_path))

    def _handle_directory_change(self, directory: Path):
        if not self.s3_manager:
            print("⚠️ S3 Tables sync not configured. Run 'ndx configure' to set up.")
            return

        try:
            table_name = directory.relative_to(self.data_dir).name
            self.s3_manager.sync_directory(directory, table_name)
        except Exception as e:
            print(f"Error syncing directory: {str(e)}")
