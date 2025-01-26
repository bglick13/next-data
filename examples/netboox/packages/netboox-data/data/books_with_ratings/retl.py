import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from nextdata.core.connections.spark import SparkManager
from nextdata.core.glue.glue_entrypoint import (
    GlueJobArgs,
    glue_job,
)

connection_name = "dsql"


@glue_job(JobArgsType=GlueJobArgs)
def main(spark_manager: SparkManager, _: GlueJobArgs) -> DataFrame:
    """
    Write the entire books data table to the database efficiently using PostgreSQL COPY command.
    """
    # spark = spark_manager.spark  # noqa: ERA001
    # books = DataTable("books", spark)  # noqa: ERA001
    # ratings = DataTable("ratings", spark)  # noqa: ERA001
    # all_books = books.df  # noqa: ERA001
    # all_ratings = ratings.df  # noqa: ERA001
    all_books = spark_manager.get_table("books")
    all_ratings = spark_manager.get_table("ratings")
    ratings_by_book = all_ratings.groupBy("isbn").agg(
        F.avg("book_rating").alias("avg_rating"),
    )
    books_with_ratings = all_books.join(ratings_by_book, on="isbn", how="left")
    spark_manager.write_to_table(
        "books_with_ratings",
        books_with_ratings,
        mode="overwrite",
    )
    return books_with_ratings


if __name__ == "__main__":
    main()  # type: ignore this is right
