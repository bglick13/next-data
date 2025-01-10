from typing import Literal
import json
import boto3
from pydantic import Field

from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs
from nextdata.core.project_config import NextDataConfig


def generate_dsql_password(host: str) -> str:
    config = NextDataConfig.from_env()
    client = boto3.client("dsql", region_name=config.aws_region)
    token = client.generate_connect_auth_token(host, config.aws_region)
    return token


class DSQLGlueJobArgs(JDBCGlueJobArgs):
    """
    Arguments for a glue job that uses a DSQL connection.
    """

    connection_type: Literal["dsql"] = "dsql"
    protocol: Literal["postgresql"] = "postgresql"
    host: str
    port: int = 5432
    database: str = "postgres"
    username: str = "admin"
    password: str = None
    required_iam_policies: dict[str, str] = {
        "dsqlconnect": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": [
                            "dsql:ListClusters",
                            "dsql:DbConnect",
                            "dsql:ListTagsForResource",
                            "dsql:GetCluster",
                        ],
                        "Effect": "Allow",
                        "Resource": ["*"],
                    }
                ],
            }
        )
    }
