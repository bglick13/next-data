from __future__ import annotations

from typing import Any, Literal

from sqlalchemy import URL, create_engine

from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)


class JDBCGlueJobArgs(GenericConnectionGlueJobArgs):
    """Arguments for a glue job that uses a JDBC connection."""

    connection_type: Literal["jdbc"] = "jdbc"
    protocol: Literal["postgresql", "mysql", "sqlserver", "oracle", "db2", "mariadb"]
    host: str
    port: int
    database: str
    username: str
    password: str | None = None


class RemoteDBConnection:
    def __init__(
        self,
        url: str | URL,
        connect_args: dict[str, Any],
        **kwargs: dict[str, Any],  # noqa: ARG002
    ) -> None:
        self.url = url
        self.engine = create_engine(url, connect_args=connect_args)
