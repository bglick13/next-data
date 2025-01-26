import datetime
from typing import Any
from nextdata.core.glue.connections.jdbc import RemoteDBConnection
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import Mapped
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from pyspark.sql import DataFrame
from sqlalchemy.orm import Session
import sqlalchemy as sa


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView)
def _create_view(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(DropView)
def _drop_view(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)


def view_exists(ddl, target, connection, **kw):
    return ddl.name in sa.inspect(connection).get_view_names()


def view_doesnt_exist(ddl, target, connection, **kw):
    return not view_exists(ddl, target, connection, **kw)


def view(name, metadata, selectable):
    t = table(name)

    t._columns._populate_separate_keys(
        col._make_proxy(t) for col in selectable.selected_columns
    )

    sa.event.listen(
        metadata,
        "after_create",
        CreateView(name, selectable).execute_if(callable_=view_doesnt_exist),
    )
    sa.event.listen(
        metadata, "before_drop", DropView(name).execute_if(callable_=view_exists)
    )
    return t


class Base(DeclarativeBase):
    pass


class RetlOutputHistory(Base):
    __tablename__ = "retl_output_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime)
    table_name: Mapped[str] = mapped_column(String)


class RetlDbConnection(RemoteDBConnection):
    def __init__(self, url: str, connect_args: dict[str, Any], **kwargs):
        super().__init__(url, connect_args)
        self.retl_output_history_table = "retl_output_history"
        self.base_table_name = kwargs["sql_table"]
        self.timestamp = datetime.datetime.now()
        self.timestamp_str = self.timestamp.strftime("%Y%m%d%H%M%S")
        self.timestamped_table_name = f"{self.base_table_name}_{self.timestamp_str}"
        # Create the retl_output_history table if it doesn't exist
        table_view = view(
            self.base_table_name,
            Base.metadata,
            sa.select("*").select_from(table(self.timestamped_table_name)),
        )
        metadata = Base.metadata
        metadata.create_all(self.engine)

    def _dataframe_to_sql_schema(self, df: DataFrame) -> str:
        """Convert a dataframe to a sql schema"""
        return f"({', '.join([f'{col} {dtype}' for col, dtype in df.dtypes])})"

    def create_table_from_df(self, df: DataFrame) -> None:
        """Create a table"""
        sql_schema = self._dataframe_to_sql_schema(df)
        with Session(self.engine) as session:
            session.execute(
                f"CREATE TABLE IF NOT EXISTS {self.timestamped_table_name} {sql_schema}"
            )

            session.commit()

    def write_to_table(self, df: DataFrame) -> None:
        """Write data to a table"""
        first_column = df.columns[0]
        df.toPandas().to_sql(
            self.timestamped_table_name,
            self.engine,
            if_exists="replace",
            chunksize=10000,
            index=True,
            index_label=first_column,
        )

    def add_to_retl_output_history(self) -> None:
        """Add a table to the retl output history"""
        with Session(self.engine) as session:
            output_history = RetlOutputHistory(
                timestamp=self.timestamp,
                table_name=self.base_table_name,
            )
            session.add(output_history)
            session.commit()

    def cleanup_old_tables(self, keep_n_tables: int) -> None:
        """Cleanup old tables"""
        with Session(self.engine) as session:
            session.execute(
                f"DELETE FROM {self.retl_output_history_table} WHERE id NOT IN (SELECT id FROM {self.retl_output_history_table} where table_name = {self.base_table_name} ORDER BY timestamp DESC LIMIT {keep_n_tables})"
            )
            session.commit()

    def write_retl_result(self, df: DataFrame) -> None:
        """Write the result of a retl job to the retl output table"""
        self.write_to_table(df)
        self.add_to_retl_output_history()
        self.cleanup_old_tables(keep_n_tables=3)
