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

    # Create a map of column names to their data types
    column_types = {
        row["column_name"]: row["data_type"].lower() for row in columns_df.collect()
    }

    # Look for best partition column in order of preference:
    # 1. Primary key or identity column
    # 2. Provided incremental column if numeric
    # 3. Any indexed numeric column
    # 4. Hash-based partitioning as fallback

    # Get primary key columns
    pk_query = f"""
    SELECT kcu.column_name, c.data_type
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.columns c
        ON kcu.column_name = c.column_name
        AND kcu.table_name = c.table_name
    WHERE tc.table_name = '{table_name}' 
        AND tc.constraint_type = 'PRIMARY KEY'
    """

    pk_df = spark_manager.spark.read.jdbc(
        url=connection_options["url"],
        table=f"({pk_query}) AS tmp",
        properties=connection_options,
    )

    if pk_df.count() > 0:
        pk_row = pk_df.first()
        partition_col = pk_row["column_name"]
        data_type = pk_row["data_type"].lower()
        numeric_types = {
            "integer",
            "bigint",
            "smallint",
            "decimal",
            "numeric",
            "real",
            "double precision",
        }
        if data_type in numeric_types:
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
        else:
            logging.info(f"Primary key column {partition_col} is not numeric")

    # Fallback to hash-based partitioning
    return PartitionStrategy(
        type="hash",
        num_partitions=10,
        predicates=[
            f"MOD(HASH(CAST(CONCAT({','.join(columns_df.select('column_name').rdd.flatMap(lambda x: x).collect())}) AS VARCHAR)), 10) = {i}"
            for i in range(10)
        ],
        column=None,
        lower_bound=None,
        upper_bound=None,
    )


logger = logging.getLogger(__name__)


@glue_job(JobArgsType=GlueJobArgs)
def main(
    spark_manager: SparkManager,
    job_args: GlueJobArgs,
):
    # Read source data into a Spark DataFrame
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
        driver="org.postgresql.Driver",
    )

    # Add more JDBC properties for debugging
    connection_options.update(
        {
            "loginTimeout": "60",  # Connection timeout in seconds
            "logLevel": "2",  # Detailed JDBC logging (1-4)
            "socketTimeout": "60",  # Socket timeout in seconds
            "connectTimeout": "60",  # Connect timeout in seconds
            "tcpKeepAlive": "true",  # Keep-alive in seconds
        }
    )

    partition_strategy = get_partition_strategy(
        spark_manager,
        connection_options,
        job_args.sql_table,
        job_args.incremental_column,
    )
    print(f"Partition strategy: {partition_strategy}")
    if partition_strategy.type == "numeric":
        source_df: DataFrame = spark_manager.spark.read.jdbc(
            url=connection_options["url"],
            table=job_args.sql_table,
            column=partition_strategy.column,
            lowerBound=partition_strategy.lower_bound,
            upperBound=partition_strategy.upper_bound,
            numPartitions=partition_strategy.num_partitions,
            properties=connection_options,
        )
    elif partition_strategy.type == "hash":
        source_df: DataFrame = (
            spark_manager.spark.read.option(
                "numPartitions", partition_strategy.num_partitions
            )
            .option("predicates", partition_strategy.predicates)
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
