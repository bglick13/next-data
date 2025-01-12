import json
import asyncclick as click
import boto3
from nextdata.core.pulumi_context_manager import PulumiContextManager


@click.group()
def aws():
    """aws commands"""
    pass


@aws.command(name="get-glue-role-token")
def get_glue_role_token(hostname: str):
    """Get a DSQL auth token by assuming the Glue role and using those credentials"""
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.initialize_stack()
    stack_outputs = pulumi_context_manager.get_stack_outputs()

    sts_client = boto3.client(
        "sts",
        aws_access_key_id=pulumi_context_manager.config.aws_access_key_id,
        aws_secret_access_key=pulumi_context_manager.config.aws_secret_access_key,
        region_name=pulumi_context_manager.config.aws_region,
    )

    assumed_role = sts_client.assume_role(
        RoleArn=stack_outputs.glue_role_arn, RoleSessionName="GlueSession"
    )

    credentials = assumed_role["Credentials"]

    dsql_client = boto3.client(
        "dsql",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=pulumi_context_manager.config.aws_region,
    )

    token = dsql_client.generate_db_connect_auth_token(
        hostname,
        pulumi_context_manager.config.aws_region,
    )

    click.echo(token)
