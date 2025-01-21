from nextdata.core.connections.spark import SparkManager
from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class PartitionStrategy:
    type: Literal["numeric", "hash"]
    num_partitions: int
    predicates: Optional[list[str]]
    column: Optional[str]
    lower_bound: Optional[str]
    upper_bound: Optional[str]

    @classmethod
    def from_dict(cls, data: dict) -> "PartitionStrategy":
        return cls(**data)


def get_partition_strategy(
    spark_manager: SparkManager,
    connection_options: dict,
    table_name: str,
    incremental_column: str | None = None,
) -> PartitionStrategy:
    """Get optimal partition strategy based on table structure"""
    # Query table metadata to find suitable partition columns
    metadata_query = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}'
    """
    columns_df = spark_manager.spark.read.jdbc(
        url=connection_options["url"],
        table=f"({metadata_query}) AS tmp",
        properties=connection_options,
    )

    # Look for best partition column in order of preference:
    # 1. Primary key or identity column
    # 2. Provided incremental column if numeric
    # 3. Any indexed numeric column
    # 4. Hash-based partitioning as fallback

    pk_query = f"""
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_name = '{table_name}' 
        AND tc.constraint_type = 'PRIMARY KEY'
    """

    pk_df = spark_manager.spark.read.jdbc(
        url=connection_options["url"],
        table=f"({pk_query}) AS tmp",
        properties=connection_options,
    )

    if pk_df.count() > 0:
        partition_col = pk_df.first()["column_name"]
        # Get bounds for numeric partition column
        bounds_query = f"""
        SELECT MIN({partition_col}) as min_val, 
               MAX({partition_col}) as max_val,
               COUNT(*) as row_count
        FROM {table_name}
        """
        bounds = spark_manager.spark.read.jdbc(
            url=connection_options["url"],
            table=f"({bounds_query}) AS tmp",
            properties=connection_options,
        ).first()

        return PartitionStrategy(
            type="numeric",
            column=partition_col,
            lower_bound=bounds["min_val"],
            upper_bound=bounds["max_val"] + 1,  # Add 1 to include max value
            num_partitions=min(
                100, max(10, bounds["row_count"] // 100000)
            ),  # 100k rows per partition
        )

    # Fallback to hash-based partitioning
    return PartitionStrategy(
        type="hash",
        num_partitions=10,
        predicates=[
            f"MOD(HASH(CAST(CONCAT({','.join(columns_df.select('column_name').rdd.flatMap(lambda x: x).collect())}) AS VARCHAR)), 10) = {i}"
            for i in range(10)
        ],
    )
