from nextdata.core.project_config import NextDataConfig
from nextdata.core.connections.spark import SparkManager
from pyspark.sql import DataFrame


class DataTable:

    def __init__(
        self,
        name: str,
        spark: SparkManager,
    ):
        self.spark = spark
        self.config = NextDataConfig.from_env()
        self.data_dir = self.config.data_dir
        available_tables = [
            file.name for file in self.data_dir.iterdir() if file.is_dir()
        ]
        self.name = name
        if name not in available_tables:
            raise ValueError(f"Table {name} not found in data directory")

    @property
    def df(self) -> DataFrame:
        return self.spark.get_table(self.name)

    @property
    def partition_keys(self) -> list[str]:
        return self.df.schema.fields[0].metadata.get("partition_keys")
