import boto3
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session

from nextdata.core.connections.base_connection import BaseConnection


class DSQLConnection(BaseConnection):
    def __init__(self):
        super().__init__()

    def create_dsql_engine(self):
        hostname = os.getenv("DSQL_CLUSTER_ENDPOINT")
        region = os.getenv("AWS_REGION")
        client = boto3.client("dsql", region_name=region)

        # The token expiration time is optional, and the default value 900 seconds
        # Use `generate_db_connect_auth_token` instead if you are not connecting as `admin` user
        password_token = client.generate_db_connect_admin_auth_token(hostname, region)

        # Example on how to create engine for SQLAlchemy
        url = URL.create(
            "postgresql",
            username="admin",
            password=password_token,
            host=hostname,
            database="postgres",
        )
        # Prefer sslmode = verify-full for production usecases
        engine = create_engine(
            url, connect_args={"sslmode": "require"}, pool_size=20, max_overflow=0
        )

        return engine

    def connect(self):
        engine = self.create_dsql_engine()
        Base = automap_base()
        Base.prepare(engine, reflect=True)
        self.Base = Base
        session = Session(engine)
        return session, engine

    def get_table(self, table_name):
        try:
            return self.Base.classes[table_name]
        except Exception as e:
            raise ValueError(f"Table {table_name} not found") from e
