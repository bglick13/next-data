from typing import Any
from pyspark.sql import functions as F
from nextdata.core.connections.spark import SparkManager
from nextdata.core.glue.connections.dsql import DSQLGlueJobArgs, generate_dsql_password
from nextdata.core.glue.glue_entrypoint import glue_job, GlueJobArgs
from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs
from pyspark.sql import DataFrame
import logging

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
    incremental_column: Optional[str] = None,
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


logger = logging.getLogger(__name__)


@glue_job(JobArgsType=GlueJobArgs)
def main(
    spark_manager: SparkManager,
    job_args: GlueJobArgs,
):
    # Read source data into a Spark DataFrame
    spark_manager.spark.sparkContext.addArchive
    base_query = f"SELECT * FROM {job_args.sql_table}"
    logger.info(f"Base query: {base_query}")
    connection_conf = None
    password = None
    if job_args.connection_type == "dsql":
        connection_args: dict[str, Any] = job_args.connection_properties
        connection_conf = DSQLGlueJobArgs(host=connection_args["host"])
        password = generate_dsql_password(connection_conf.host)
    elif job_args.connection_type == "jdbc":
        connection_conf = JDBCGlueJobArgs(**job_args.connection_properties)
        password = connection_conf.password
    else:
        raise ValueError(f"Unsupported connection type: {job_args.connection_type}")

    connection_options = dict(
        url=f"jdbc:{connection_conf.protocol}://{connection_conf.host}:{connection_conf.port}/{connection_conf.database}",
        dbtable=job_args.sql_table,
        user=connection_conf.username,
        password=password,
        ssl="true",
        sslmode="require",
        driver="com.postgresql.jdbc.Driver",
    )

    partition_strategy = get_partition_strategy(
        spark_manager,
        connection_options,
        job_args.sql_table,
        job_args.incremental_column,
    )
    logger.info(f"Partition strategy: {partition_strategy}")

    if partition_strategy.type == "numeric":
        source_df: DataFrame = spark_manager.spark.read.jdbc(
            url=connection_options["url"],
            table=job_args.sql_table,
            column=partition_strategy["column"],
            lowerBound=partition_strategy["lowerBound"],
            upperBound=partition_strategy["upperBound"],
            numPartitions=partition_strategy["numPartitions"],
            properties=connection_options,
        )
    elif partition_strategy.type == "hash":
        source_df: DataFrame = (
            spark_manager.spark.read.option(
                "numPartitions", partition_strategy["numPartitions"]
            )
            .option("predicates", partition_strategy["predicates"])
            .jdbc(
                url=connection_options["url"],
                table=job_args.sql_table,
                properties=connection_options,
            )
        )
    logger.info(f"# of rows: {source_df.count()}")
    source_df.show()
    # Register the DataFrame as a temp view to use with Spark SQL
    source_df = source_df.withColumn("ds", F.current_date())

    spark_manager.write_to_table(
        table_name=job_args.sql_table,
        df=source_df,
        mode="overwrite" if job_args.is_full_load else "append",
    )


if __name__ == "__main__":
    main()
