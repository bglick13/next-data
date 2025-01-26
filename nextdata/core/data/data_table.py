from __future__ import annotations

from typing import TYPE_CHECKING

from nextdata.core.project_config import NextDataConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from nextdata.core.connections.spark import SparkManager


class DataTable:
    def __init__(
        self,
        name: str,
        spark: SparkManager,
    ) -> None:
        self.spark = spark
        self.name = name
        try:
            self.config = NextDataConfig.from_env()
            if self.config:
                self.data_dir = self.config.data_dir
                available_tables = [file.name for file in self.data_dir.iterdir() if file.is_dir()]
                if name not in available_tables:
                    raise ValueError  # noqa: TRY301
        except Exception:  # noqa: BLE001, S110
            pass

    @property
    def df(self) -> DataFrame:
        return self.spark.get_table(self.name)

    @property
    def partition_keys(self) -> list[str]:
        res = self.df.schema.fields[0].metadata.get("partition_keys")
        if res is None:
            return []
        return res
