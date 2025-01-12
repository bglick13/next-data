from typing import Any
from pyspark.sql import functions as F
from nextdata.core.connections.spark import SparkManager
from nextdata.core.glue.connections.dsql import DSQLGlueJobArgs, generate_dsql_password
from nextdata.core.glue.glue_entrypoint import glue_job, GlueJobArgs
from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs
from pyspark.sql import DataFrame


@glue_job(job_args_type=GlueJobArgs)
def main(
    *args,
    **kwargs,
):
    spark_manager: SparkManager = kwargs["spark_manager"]
    glue_context = kwargs["glue_context"]
    job = kwargs["job"]
    job_args: GlueJobArgs = kwargs["job_args"]
    logger = glue_context.get_logger()
    # Read source data into a Spark DataFrame
    if job_args.SQLQuery.strip():
        base_query = job_args.SQLQuery
    else:
        base_query = f"SELECT * FROM {job_args.SQLTable}"
    logger.info(f"Base query: {base_query}")
    connection_conf = None
    password = None
    if job_args.ConnectionType == "dsql":
        connection_args: dict[str, Any] = job_args.ConnectionProperties
        connection_conf = DSQLGlueJobArgs(host=connection_args["host"])
        password = generate_dsql_password(connection_conf.host)
    elif job_args.ConnectionType == "jdbc":
        connection_conf = JDBCGlueJobArgs(**job_args.ConnectionProperties)
        password = connection_conf.password
    else:
        raise ValueError(f"Unsupported connection type: {job_args.ConnectionType}")

    connection_options = dict(
        url=f"jdbc:{connection_conf.protocol}://{connection_conf.host}:{connection_conf.port}/{connection_conf.database}",
        dbtable=job_args.SQLTable,
        user=connection_conf.username,
        password=password,
        ssl=True,
        sslmode="require",
        # driver="com.amazon.dsql.jdbc.Driver",
    )
    source_df: DataFrame = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options=connection_options,
    ).toDF()
    logger.info(f"# of rows: {source_df.count()}")
    source_df.show()
    # Register the DataFrame as a temp view to use with Spark SQL
    source_df = source_df.withColumn("ds", F.current_date())

    spark_manager.write_to_table(
        table_name=job_args.SQLTable,
        df=source_df,
        mode="overwrite" if job_args.IsFullLoad else "append",
    )


if __name__ == "__main__":
    main()
