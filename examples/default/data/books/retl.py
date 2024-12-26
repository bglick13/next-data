from examples.default.connections.dsql.main import DSQLConnection
from nextdata.core.connections.spark import SparkManager
from nextdata.core.data.data_table import DataTable
from pyspark.sql import DataFrame
import tempfile
import os
from sqlalchemy import text


def main():
    """
    Write the entire books data table to the database efficiently using PostgreSQL COPY command.
    """
    spark = SparkManager().spark
    books = DataTable("books", spark)
    all_books = books.df

    # Create a temporary directory for CSV files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Group by partition keys and process each partition
        grouped_books = all_books.groupBy(books.partition_keys)
        _, engine = DSQLConnection().connect()
        # Get raw connection for COPY command
        with engine.connect() as conn:
            for partition in grouped_books:
                partition_df: DataFrame = partition[1]

                # Write partition to temporary CSV
                temp_csv = os.path.join(
                    temp_dir, f"partition_{hash(str(partition[0]))}.csv"
                )
                partition_df.toPandas().to_csv(temp_csv, index=False, header=False)

                # Use COPY command to bulk load the data
                with open(temp_csv, "r") as f:
                    # Start a transaction
                    with conn.begin():
                        # Create a raw COPY command
                        copy_sql = text(
                            f"""
                            COPY books ({','.join(partition_df.columns)})
                            FROM STDIN WITH (FORMAT CSV)
                            """
                        )
                        # Execute COPY command
                        conn.connection.cursor().copy_expert(copy_sql, f)


if __name__ == "__main__":
    main()
