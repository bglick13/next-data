from typing import Literal
import json
import boto3

from nextdata.core.glue.connections.jdbc import JDBCGlueJobArgs
from nextdata.core.project_config import NextDataConfig


def generate_dsql_password(host: str) -> str:
    config = NextDataConfig.from_env()
    region = config.aws_region if config else "us-east-1"
    client = boto3.client("dsql", region_name=region)
    token = client.generate_db_connect_admin_auth_token(host, region)
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
                            "dsql:DbConnectAdmin",
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
