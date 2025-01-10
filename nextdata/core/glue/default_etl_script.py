from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from nextdata.core.glue.connections.dsql import DSQLGlueJobArgs, generate_dsql_password
from nextdata.core.glue.glue_entrypoint import glue_job, GlueJobArgs
from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs


@glue_job(job_args_type=GlueJobArgs)
def main(
    *args,
    **kwargs,
):
    spark: SparkContext = kwargs["spark"]
    glue_context = kwargs["glue_context"]
    job = kwargs["job"]
    job_args: GlueJobArgs = kwargs["job_args"]
    # Read source data into a Spark DataFrame
    if job_args.SQLQuery:
        base_query = job_args.SQLQuery
    else:
        base_query = f"SELECT * FROM {job_args.SQLTable}"
    connection_conf = None
    password = None
    if job_args.ConnectionType == "dsql":
        connection_conf = DSQLGlueJobArgs(**job_args.ConnectionProperties)
        password = generate_dsql_password(connection_conf.host)
    elif job_args.ConnectionType == "jdbc":
        connection_conf = JDBCGlueJobArgs(**job_args.ConnectionProperties)
        password = connection_conf.password
    else:
        raise ValueError(f"Unsupported connection type: {job_args.ConnectionType}")
    sql_context = SQLContext(spark)
    source_df = (
        sql_context.read.format("jdbc")
        .options(
            url=f"jdbc:{connection_conf.protocol}://{connection_conf.host}:{connection_conf.port}/{connection_conf.database}",
            query=base_query,
            user=connection_conf.username,
            password=password,
        )
        .load()
    )

    # Register the DataFrame as a temp view to use with Spark SQL
    source_df = source_df.withColumn("ds", F.current_date())
    source_df.createOrReplaceTempView("source_data")

    # Get the schema from the source DataFrame
    schema_columns = ", ".join(
        [
            f"{field.name} {field.dataType.simpleString()}"
            for field in source_df.schema.fields
        ]
        + ["ds timestamp"]
    )

    # Create the Iceberg table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {job_args.OutputS3Path} (
        {schema_columns}
    )
    USING iceberg
    """
    spark.sql(create_table_sql)

    # Insert or merge data based on incremental settings
    if not job_args.IsFullLoad and job_args.IncrementalColumn:
        # Merge for incremental updates
        merge_sql = f"""
        MERGE INTO {job_args.OutputS3Path} target
        USING source_data source
        ON source.{job_args.IncrementalColumn} = target.{job_args.IncrementalColumn}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    else:
        # Full load - overwrite the table
        insert_sql = f"""
        INSERT OVERWRITE INTO {job_args.OutputS3Path}
        SELECT * FROM source_data
        """
        spark.sql(insert_sql)


if __name__ == "__main__":
    main()
