from nextdata.core.connections.spark import SparkManager
from nextdata.core.data.data_table import DataTable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from nextdata.core.glue.glue_entrypoint import (
    GlueJobArgs,
    glue_job,
)

connection_name = "dsql"


@glue_job(JobArgsType=GlueJobArgs)
def main(spark_manager: SparkManager, job_args: GlueJobArgs) -> DataFrame:
    """
    Write the entire books data table to the database efficiently using PostgreSQL COPY command.
    """
    spark = spark_manager.spark
    # books = DataTable("books", spark)
    # ratings = DataTable("ratings", spark)
    # all_books = books.df
    # all_ratings = ratings.df
    all_books = spark_manager.get_table("books")
    all_ratings = spark_manager.get_table("ratings")
    ratings_by_book = all_ratings.groupBy("isbn").agg(
        F.avg("book_rating").alias("avg_rating")
    )
    books_with_ratings = all_books.join(ratings_by_book, on="isbn", how="left")
    spark_manager.write_to_table(
        "books_with_ratings", books_with_ratings, mode="overwrite"
    )
    return books_with_ratings


if __name__ == "__main__":
    main()
