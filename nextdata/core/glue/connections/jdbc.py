from typing import Literal, Optional

from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)


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
