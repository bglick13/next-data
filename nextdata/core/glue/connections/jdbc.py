from typing import Any, Literal, Optional

from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)
from sqlalchemy import create_engine
from pyspark.sql import DataFrame


class JDBCGlueJobArgs(GenericConnectionGlueJobArgs):
    """
    Arguments for a glue job that uses a JDBC connection.
    """

    connection_type: Literal["jdbc"] = "jdbc"
    protocol: Literal["postgresql", "mysql", "sqlserver", "oracle", "db2", "mariadb"]
    host: str
    port: int
    database: str
    username: str
    password: Optional[str] = None


class RemoteDBConnection:
    def __init__(self, url: str, connect_args: dict[str, Any], **kwargs):
        self.url = url
        self.engine = create_engine(url, connect_args=connect_args)
