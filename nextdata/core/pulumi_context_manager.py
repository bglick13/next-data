import json
import subprocess
from typing import Literal
import click
import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
from pathlib import Path

import os

from nextdata.cli.types import StackOutputs
import importlib.util
import importlib.resources


from nextdata.core.db.db_manager import DatabaseManager
from nextdata.core.db.models import (
    AwsResource,
    ConnectionType,
    EmrJob,
    EmrJobScript,
    HumanReadableName,
    JobType,
    S3DataTable,
)
from nextdata.util.framework_magic import (
    get_connection_args,
    get_connection_name,
    get_incremental_column,
    get_input_tables,
    has_custom_glue_job,
)

from .project_config import NextDataConfig
import boto3


def get_aws_identity():
    sts_client = boto3.client("sts")
    identity = sts_client.get_caller_identity()

    account_id = identity["Account"]
    # The ARN will be in format: arn:aws:iam::ACCOUNT_ID:user/USERNAME
    # or arn:aws:sts::ACCOUNT_ID:assumed-role/ROLE_NAME/SESSION_NAME
    arn = identity["Arn"]

    # Parse the username or role name from the ARN
    if "/user/" in arn:
        principal_name = arn.split("/user/")[-1]
        principal_type = "user"
    elif "/assumed-role/" in arn:
        principal_name = arn.split("/assumed-role/")[-1].split("/")[0]
        principal_type = "role"
    else:
        principal_name = arn
        principal_type = "unknown"

    return {
        "account_id": account_id,
        "arn": arn,
        "principal_name": principal_name,
        "principal_type": principal_type,
    }


"""
Handles the creation and management of Pulumi stack and AWS resources.
1. IAM user for S3, Glue, and Athena
2. S3 bucket for tables
3. Glue catalog for tables
    - S3 bucket for glue scripts
4. Athena database for tables
"""


class PulumiContextManager:
    def __init__(self):
        self.config = NextDataConfig.from_env()
        db_path = self.config.project_dir / "nextdata.db"
        self.db_manager = DatabaseManager(db_path)

        self._stack = None
        self._table_bucket = None
        self._table_namespace = None
        self._tables: dict[str, S3DataTable] = {}  # Keep track of tables by name
        self._iam_role = None
        self._iam_role_policy_attachment_s3 = None
        self._iam_role_policy_attachment_glue = None
        self._iam_role_policy_attachment_athena = None
        self._iam_s3_policy = None
        self._iam_glue_policy = None
        self._iam_athena_policy = None
        self._glue_catalog_database = None
        self._glue_job_bucket = None
        self._glue_etl_job_script = None

    @property
    def iam_role(self) -> aws.iam.Role:
        if not self._iam_role:
            self._create_iam_resources()
        return self._iam_role

    @property
    def iam_role_policy_attachment_s3(self) -> aws.iam.RolePolicyAttachment:
        if not self._iam_role_policy_attachment_s3:
            self._create_iam_resources()
        return self._iam_role_policy_attachment_s3

    @property
    def glue_catalog_database(self) -> aws.glue.CatalogDatabase:
        if not self._glue_catalog_database:
            self._setup_glue()
        return self._glue_catalog_database

    @property
    def glue_job_bucket(self) -> aws.s3.BucketV2:
        if not self._glue_job_bucket:
            self._setup_glue()
        return self._glue_job_bucket

    @property
    def glue_etl_job_script(self) -> aws.s3.BucketObject:
        if not self._glue_etl_job_script:
            self._setup_glue()
        return self._glue_etl_job_script

    @property
    def stack(self) -> auto.Stack:
        if not self._stack:
            self.initialize_stack()
        return self._stack

    @property
    def table_bucket(self) -> aws.s3tables.TableBucket:
        if not self._table_bucket:
            self.initialize_stack()
        return self._table_bucket

    @property
    def table_namespace(self) -> aws.s3tables.Namespace:
        if not self._table_namespace:
            self.initialize_stack()
        return self._table_namespace

    def initialize_stack(self):
        """Initialize or get existing stack"""
        if not self._stack:
            self._stack = auto.create_or_select_stack(
                stack_name=self.config.stack_name,
                project_name=self.config.project_name.lower().replace("-", "_"),
                program=self._construct_pulumi_program,
            )
            self.stack.workspace.install_plugin("aws", "v6.66.0")
            self.stack.set_config(
                "aws:region", auto.ConfigValue(self.config.aws_region)
            )

    def handle_table_creation(self, table_path: str):
        """Handle table creation"""
        self._stack = auto.create_or_select_stack(
            stack_name=self.config.stack_name,
            project_name=self.config.project_name.lower().replace("-", "_"),
            program=self._construct_pulumi_program,
        )
        self._stack.up(on_output=lambda msg: click.echo(f"Pulumi: {msg}"))

    def _create_iam_resources(self):
        """Create an IAM role for the stack"""
        identity = get_aws_identity()
        glue_role = aws.iam.Role(
            "glue-role",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "emr-serverless.amazonaws.com"},
                        },
                        # Allow the principal to assume the role - this will be used to trigger EMR Serverless jobs from the dashboard
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"AWS": identity["arn"]},
                        },
                    ],
                }
            ),
        )

        s3_policy = aws.iam.Policy(
            "s3-policy",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Action": ["s3:*"], "Effect": "Allow", "Resource": ["*"]},
                    ],
                }
            ),
        )

        s3_tables_policy = aws.iam.RolePolicy(
            "s3-tables-policy",
            role=glue_role.id,
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "s3tables:*",
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject",
                            ],
                            "Effect": "Allow",
                            "Resource": ["*"],
                        }
                    ],
                }
            ),
        )

        # Add glue policy
        glue_policy = aws.iam.Policy(
            "glue-policy",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": ["glue:*", "glue:PassConnection"],
                            "Effect": "Allow",
                            "Resource": ["*"],
                        }
                    ],
                }
            ),
        )

        execution_policy = aws.iam.RolePolicy(
            "execution-role-policy",
            role=glue_role.id,
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "emr-serverless:*",
                                "emr:StartJobRun",
                                "iam:PassRole",
                            ],
                            "Effect": "Allow",
                            "Resource": ["*"],
                        }
                    ],
                }
            ),
        )

        athena_policy = aws.iam.Policy(
            "athena-policy",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Action": ["athena:*"], "Effect": "Allow", "Resource": ["*"]}
                    ],
                }
            ),
        )

        lakeformation_policy = aws.iam.Policy(
            "lakeformation-policy",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": [
                                "lakeformation:RegisterResource",
                                "lakeformation:*",
                            ],
                            "Effect": "Allow",
                            "Resource": ["*"],
                        }
                    ],
                }
            ),
        )
        # Add CloudWatch Logs permissions
        cloudwatch_policy = aws.iam.Policy(
            "cloudwatch-policy",
            policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                                "logs:AssociateKmsKey",
                                "logs:GetLogEvents",
                                "logs:DescribeLogStreams",
                                "logs:DescribeLogGroups",
                            ],
                            "Resource": [
                                "arn:aws:logs:*:*:/aws-glue/*",
                                "arn:aws:logs:*:*:/aws-emr-serverless-logs/*",
                            ],
                        }
                    ],
                }
            ),
        )

        role_policy_attachment_cloudwatch = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-cloudwatch",
            role=glue_role.name,
            policy_arn=cloudwatch_policy.arn,
        )

        role_policy_attachment_s3 = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-s3",
            role=glue_role.name,
            policy_arn=s3_policy.arn,
        )
        role_policy_attachment_glue = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-glue",
            role=glue_role.name,
            policy_arn=glue_policy.arn,
        )
        role_policy_attachment_athena = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-athena",
            role=glue_role.name,
            policy_arn=athena_policy.arn,
        )
        role_policy_attachment_lakeformation = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-lakeformation",
            role=glue_role.name,
            policy_arn=lakeformation_policy.arn,
        )
        role_policy_attachment_lakeformation_admin = aws.iam.RolePolicyAttachment(
            "role-policy-attachment-lakeformation-admin",
            role=glue_role.name,
            policy_arn="arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin",
        )
        self._iam_role = glue_role
        pulumi.Output.all(
            name=glue_role.name,
            resource_type="iam_role",
            resource_id=glue_role.id,
            resource_arn=glue_role.arn,
        ).apply(
            lambda args: self.db_manager.add_resource(
                AwsResource(
                    name=args["name"],
                    human_readable_name=HumanReadableName.GLUE_ROLE,
                    resource_type=args["resource_type"],
                    resource_id=args["resource_id"],
                    resource_arn=args["resource_arn"],
                )
            )
        )
        self._iam_role_policy_attachment_s3 = role_policy_attachment_s3
        self._iam_role_policy_attachment_glue = role_policy_attachment_glue
        self._iam_role_policy_attachment_athena = role_policy_attachment_athena
        self._iam_s3_policy = s3_policy
        self._iam_glue_policy = glue_policy
        self._iam_athena_policy = athena_policy

        # Check all configured connection types. If they require IAM policies, assign them to the role
        for connection_name in self.config.get_available_connections():
            connection_args = get_connection_args(
                connection_name, self.config.connections_dir
            )
            if connection_args.required_iam_policies:
                for (
                    policy_name,
                    policy_json,
                ) in connection_args.required_iam_policies.items():
                    policy = aws.iam.Policy(
                        f"iam-policy-{connection_name}-{policy_name}",
                        policy=policy_json,
                    )
                    role_policy_attachment = aws.iam.RolePolicyAttachment(
                        f"role-policy-attachment-{connection_name}-{policy_name}",
                        role=glue_role.name,
                        policy_arn=policy.arn,
                    )

    def _setup_vpc(self):
        """Create VPC and networking resources for EMR Serverless"""
        # Create VPC
        vpc = aws.ec2.Vpc(
            "emr-vpc",
            cidr_block="10.0.0.0/16",
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        # Create an internet gateway
        igw = aws.ec2.InternetGateway(
            "emr-igw",
            vpc_id=vpc.id,
        )

        # Create public and private subnets across two AZs
        public_subnet_1 = aws.ec2.Subnet(
            "emr-public-subnet-1",
            vpc_id=vpc.id,
            cidr_block="10.0.1.0/24",
            availability_zone=f"{self.config.aws_region}a",
            map_public_ip_on_launch=True,
        )

        public_subnet_2 = aws.ec2.Subnet(
            "emr-public-subnet-2",
            vpc_id=vpc.id,
            cidr_block="10.0.2.0/24",
            availability_zone=f"{self.config.aws_region}b",
            map_public_ip_on_launch=True,
        )

        private_subnet_1 = aws.ec2.Subnet(
            "emr-private-subnet-1",
            vpc_id=vpc.id,
            cidr_block="10.0.3.0/24",
            availability_zone=f"{self.config.aws_region}a",
        )

        private_subnet_2 = aws.ec2.Subnet(
            "emr-private-subnet-2",
            vpc_id=vpc.id,
            cidr_block="10.0.4.0/24",
            availability_zone=f"{self.config.aws_region}b",
        )

        # Create an EIP for NAT Gateway
        eip = aws.ec2.Eip("emr-nat-eip")

        # Create NAT Gateway in the public subnet
        nat_gateway = aws.ec2.NatGateway(
            "emr-nat-gateway",
            subnet_id=public_subnet_1.id,
            allocation_id=eip.id,
        )

        # Create route tables
        public_rt = aws.ec2.RouteTable(
            "emr-public-rt",
            vpc_id=vpc.id,
            routes=[
                aws.ec2.RouteTableRouteArgs(
                    cidr_block="0.0.0.0/0",
                    gateway_id=igw.id,
                ),
            ],
        )

        private_rt = aws.ec2.RouteTable(
            "emr-private-rt",
            vpc_id=vpc.id,
            routes=[
                aws.ec2.RouteTableRouteArgs(
                    cidr_block="0.0.0.0/0",
                    nat_gateway_id=nat_gateway.id,
                ),
            ],
        )

        # Associate route tables with subnets
        public_rta_1 = aws.ec2.RouteTableAssociation(
            "emr-public-rta-1",
            subnet_id=public_subnet_1.id,
            route_table_id=public_rt.id,
        )

        public_rta_2 = aws.ec2.RouteTableAssociation(
            "emr-public-rta-2",
            subnet_id=public_subnet_2.id,
            route_table_id=public_rt.id,
        )

        private_rta_1 = aws.ec2.RouteTableAssociation(
            "emr-private-rta-1",
            subnet_id=private_subnet_1.id,
            route_table_id=private_rt.id,
        )

        private_rta_2 = aws.ec2.RouteTableAssociation(
            "emr-private-rta-2",
            subnet_id=private_subnet_2.id,
            route_table_id=private_rt.id,
        )

        # Create security group for EMR Serverless
        emr_sg = aws.ec2.SecurityGroup(
            "emr-security-group",
            vpc_id=vpc.id,
            description="Security group for EMR Serverless",
            egress=[
                aws.ec2.SecurityGroupEgressArgs(
                    from_port=0,
                    to_port=0,
                    protocol="-1",
                    cidr_blocks=["0.0.0.0/0"],
                ),
            ],
        )

        return {
            "vpc": vpc,
            "private_subnets": [private_subnet_1, private_subnet_2],
            "security_group": emr_sg,
        }

    def _setup_glue(self):
        # Setup VPC and networking
        network = self._setup_vpc()

        # Create EMR Serverless application with VPC configuration
        emr_app = aws.emrserverless.Application(
            "emr-app",
            name=f"{self.config.project_slug}-app",
            type="SPARK",
            release_label="emr-7.5.0",
            maximum_capacity=aws.emrserverless.ApplicationMaximumCapacityArgs(
                cpu="16 vCPU",
                memory="128 GB",
            ),
            initial_capacities=[
                aws.emrserverless.ApplicationInitialCapacityArgs(
                    initial_capacity_type="DRIVER",
                    initial_capacity_config=aws.emrserverless.ApplicationInitialCapacityInitialCapacityConfigArgs(
                        worker_count=1,
                        worker_configuration=aws.emrserverless.ApplicationInitialCapacityInitialCapacityConfigWorkerConfigurationArgs(
                            cpu="1 vCPU",
                            memory="4 GB",
                        ),
                    ),
                ),
            ],
            network_configuration=aws.emrserverless.ApplicationNetworkConfigurationArgs(
                subnet_ids=[subnet.id for subnet in network["private_subnets"]],
                security_group_ids=[network["security_group"].id],
            ),
        )
        pulumi.Output.all(
            name=emr_app.name,
            resource_type="emr_app",
            human_readable_name=HumanReadableName.EMR_APP,
            resource_id=emr_app.id,
            resource_arn=emr_app.arn,
        ).apply(lambda args: self.db_manager.add_resource(AwsResource(**args)))
        # emr_studio = aws.emr.Studio(
        #     "emr-studio",
        #     name=f"{self.config.project_slug}-studio",
        #     auth_mode="IAM",
        #     vpc_id=self.vpc.id,
        #     subnet_ids=self.subnets.ids,
        # )
        # Create a bucket for Glue jobs
        glue_job_bucket = aws.s3.BucketV2(
            "glue-job-bucket",
            force_destroy=True,
        )
        pulumi.Output.all(
            name=glue_job_bucket.bucket,
            human_readable_name=HumanReadableName.GLUE_JOB_BUCKET,
            resource_type="s3_bucket",
            resource_id=glue_job_bucket.id,
            resource_arn=glue_job_bucket.arn,
        ).apply(lambda args: self.db_manager.add_resource(AwsResource(**args)))
        # Add bucket policy to allow Glue to access scripts
        glue_job_bucket_policy = aws.s3.BucketPolicy(
            "glue-job-bucket-policy",
            bucket=glue_job_bucket.id,
            policy=pulumi.Output.all(bucket=glue_job_bucket.id).apply(
                lambda args: json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Sid": "AllowGlueAccess",
                                "Effect": "Allow",
                                "Principal": {"Service": "glue.amazonaws.com"},
                                "Action": [
                                    "s3:GetObject",
                                    "s3:PutObject",
                                    "s3:DeleteObject",
                                ],
                                "Resource": [
                                    f"arn:aws:s3:::{args['bucket']}/*",
                                    f"arn:aws:s3:::{args['bucket']}",
                                ],
                            }
                        ],
                    }
                )
            ),
        )
        # Upload a Glue job script.
        # default_etl_script.py is a package module in the ndx-etl package
        glue_etl_job_script = aws.s3.BucketObject(
            "glue-etl-job-script.py",
            bucket=glue_job_bucket.id,
            key="scripts/default_etl_script.py",
            source=pulumi.asset.FileAsset(
                importlib.resources.files("nextdata")
                / "core"
                / "glue"
                / "default_etl_script.py"
            ),
            opts=pulumi.ResourceOptions(depends_on=[glue_job_bucket]),
        )
        pulumi.Output.all(
            name="scripts/default_etl_script.py",
            s3_path="scripts/default_etl_script.py",
            bucket=glue_job_bucket.bucket,
        ).apply(lambda args: self.db_manager.add_script(EmrJobScript(**args)))
        # self._glue_catalog_database = glue_catalog_database
        pulumi.export("emr-app", emr_app.name)
        pulumi.export("emr-app-arn", emr_app.arn)
        pulumi.export("glue-job-bucket", glue_job_bucket.bucket)
        pulumi.export("glue-job-bucket-arn", glue_job_bucket.arn)
        pulumi.export("glue-etl-job-script", glue_etl_job_script.key)
        self._glue_job_bucket = glue_job_bucket
        self._glue_etl_job_script = glue_etl_job_script

    def _ensure_base_resources(self):
        """Ensure bucket and namespace exist"""
        if not self._iam_role:
            self._create_iam_resources()
        if not self._table_bucket:
            bucket_name = f"{self.config.project_slug}tables"
            self._table_bucket = aws.s3tables.TableBucket(
                bucket_name,
                name=bucket_name,
            )
            pulumi.Output.all(
                name=bucket_name,
                human_readable_name=HumanReadableName.S3_TABLE_BUCKET,
                resource_type="s3_table_bucket",
                resource_id=self._table_bucket.id,
                resource_arn=self._table_bucket.arn,
            ).apply(lambda args: self.db_manager.add_resource(AwsResource(**args)))

        if not self._table_namespace:
            namespace_name = f"{self.config.project_slug}namespace"
            self._table_namespace = aws.s3tables.Namespace(
                namespace_name,
                namespace=namespace_name,
                table_bucket_arn=self._table_bucket.arn,
            )
            pulumi.Output.all(
                name=namespace_name,
                human_readable_name=HumanReadableName.S3_TABLE_NAMESPACE,
                resource_type="s3_table_namespace",
                resource_id=self._table_namespace.id,
                resource_arn="",
            ).apply(lambda args: self.db_manager.add_resource(AwsResource(**args)))

    def _ensure_existing_tables(self):
        """Ensure tables exist"""
        for table_path in self.config.get_available_tables():
            table_name = table_path
            self._create_table(table_name)

    def _create_table(self, table_path: str):
        """Create a single table and update the stack"""
        table_name = Path(table_path).name
        # Convert any non-alphanumeric characters to underscores
        safe_name = "".join(c if c.isalnum() else "_" for c in table_name.lower())
        # Create the new table
        table = aws.s3tables.Table(
            safe_name,
            name=safe_name,  # Use safe name for both resource and table name
            table_bucket_arn=self._table_bucket.arn,
            namespace=self._table_namespace.namespace.apply(
                lambda ns: ns.replace("-", "_")
            ),
            format="ICEBERG",
        )
        pulumi.Output.all(name=safe_name).apply(
            lambda args: self.db_manager.add_table(S3DataTable(**args))
        )

        # Export the table location
        pulumi.export(f"table_{safe_name}", table.warehouse_location)

        # Store the table reference
        self._tables[safe_name] = table
        click.echo(f"Creating table for {table_name}")

    def _setup_lakeformation(self):
        """Grant lakeformation permissions to the principal so analytics integration works"""
        # 1. Create Lake Formation service role
        lake_formation_service_role = aws.iam.Role(
            "lake-formation-service-role",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lakeformation.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )

        # 2. Register resources with Lake Formation
        lake_formation_resource = aws.lakeformation.Resource(
            "table-bucket-registration",
            role_arn=lake_formation_service_role.arn,
            arn=self.table_bucket.arn,
            use_service_linked_role=True,
        )

        # 3. Grant Lake Formation permissions for existing Glue database
        database_permissions = aws.lakeformation.Permissions(
            "lakeformation-database-permissions",
            principal=lake_formation_service_role.arn,
            database=aws.lakeformation.PermissionsDatabaseArgs(
                name=self.glue_catalog_database.name,
                catalog_id=self.table_bucket.owner_account_id,
            ),
            permissions=["ALL"],
            opts=pulumi.ResourceOptions(depends_on=[self.glue_catalog_database]),
        )

        # 4. Grant table permissions for each table
        for table_path in self.config.get_available_tables():
            table_name = Path(table_path).name
            table_permissions = aws.lakeformation.Permissions(
                f"lakeformation-permissions-{table_name}",
                principal=lake_formation_service_role.arn,
                table=aws.lakeformation.PermissionsTableArgs(
                    database_name=self.glue_catalog_database.name,
                    name=table_name,
                    catalog_id=self.table_bucket.owner_account_id,
                ),
                permissions=["ALL"],
                opts=pulumi.ResourceOptions(depends_on=[self.glue_catalog_database]),
            )

        # # Grant table permissions for each table
        # for table_name, table in self._tables.items():
        #     table_permissions = aws.lakeformation.Permissions(
        #         f"lakeformation-permissions-{table_name}",
        #         principal=self.iam_role.arn,
        #         table=aws.lakeformation.PermissionsTableArgs(
        #             database_name=self._table_namespace.namespace,
        #             name=table_name,
        #             catalog_id=self.table_bucket.owner_account_id,
        #         ),
        #         permissions=["ALL"],
        #         opts=pulumi.ResourceOptions(depends_on=[lakeformation_settings]),
        #     )

    def _get_glue_job_bucket_name(self):
        return pulumi.Output.all(bucket=self.glue_job_bucket.bucket).apply(
            lambda args: args["bucket"]
        )

    def _package_requirements(self, requirements: str, job_name: str) -> str:
        """
        Package requirements into a virtualenv using Docker and upload to S3.
        Returns the S3 path to the packaged venv.
        """
        venv_dir = "."
        os.makedirs(venv_dir, exist_ok=True)

        # Write requirements to a file
        requirements_path = os.path.join(venv_dir, "requirements.txt")
        with open(requirements_path, "w") as f:
            f.write(requirements)
            f.write("\nvenv-pack==0.2.0")

        # Create Dockerfile that copies and installs requirements file
        dockerfile_content = """
# syntax=docker/dockerfile:1.4
FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2023-minimal AS builder

RUN dnf install -y gcc python3 python3-devel
ENV VIRTUAL_ENV=/opt/venv

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python3 -m pip install --upgrade pip

COPY requirements.txt /requirements.txt
RUN python3 -m pip install -r /requirements.txt

RUN mkdir /output && venv-pack -o /output/venv.tar.gz

FROM scratch
COPY --from=builder /output/venv.tar.gz /
"""
        dockerfile_path = "./Dockerfile"
        with open(dockerfile_path, "w") as f:
            f.write(dockerfile_content)

        # Set up BuildKit output directory
        os.environ["DOCKER_BUILDKIT"] = "1"
        docker_build_command = (
            f"docker build -t venv-builder:latest -f {dockerfile_path} --output . ."
        )
        subprocess.run(docker_build_command, shell=True, check=True)

        # The venv.tar.gz file should now be in the output directory
        venv_path = "./venv.tar.gz"
        print(f"venv_path: {venv_path}")
        if not os.path.exists(venv_path):
            raise Exception("Failed to extract venv.tar.gz from Docker build")

        # Upload to S3
        venv_key = (
            f"venvs/{self.config.project_slug}-{job_name}-{hash(requirements)}.tar.gz"
        )
        venv_object = aws.s3.BucketObject(
            f"venv-{job_name}-{hash(requirements)}",
            bucket=self.glue_job_bucket.id,
            key=venv_key,
            source=pulumi.asset.FileAsset(venv_path),
            opts=pulumi.ResourceOptions(depends_on=[self.glue_job_bucket]),
        )

        return venv_key

    def _setup_glue_job(self, table_path: Path, job_type: Literal["etl", "retl"]):
        """Setup a glue job for a table"""
        # Check if there's a custom etl script for this table by looking for an etl.py file with a @glue_job decorator
        bucket_name = self.glue_job_bucket.bucket
        table_namespace = self.table_namespace.namespace
        if has_custom_glue_job(table_path / f"{job_type}.py"):
            script_key = f"scripts/{table_path.name}/{job_type}.py"
            custom_script = aws.s3.BucketObject(
                f"glue-etl-job-script-{table_path.name}.py",
                bucket=self.glue_job_bucket.id,
                key=script_key,
                source=pulumi.asset.FileAsset(table_path / f"{job_type}.py"),
                opts=pulumi.ResourceOptions(depends_on=[self.glue_job_bucket]),
            )
            pulumi.Output.all(
                name=script_key,
                s3_path=script_key,
                bucket=bucket_name,
            ).apply(lambda args: self.db_manager.add_script(EmrJobScript(**args)))
        else:
            script_key = "scripts/default_etl_script.py"

        # Get the connection name from the etl.py file by checking connection_name variable
        connection_name = get_connection_name(table_path / f"{job_type}.py")
        if (
            not connection_name
            or connection_name not in self.config.get_available_connections()
        ):
            raise ValueError(
                f"No connection name found in {script_key}. Please add a connection_name variable and ensure it's defined in the connections directory."
            )
        connection_args = get_connection_args(
            connection_name, self.config.connections_dir
        )

        # Package requirements and get S3 path
        with open(Path(__file__).parent / "glue" / "requirements.txt") as f:
            requirements = f.read()
            venv_s3_path = self._package_requirements(requirements, table_path.name)

        incremental_column = get_incremental_column(table_path / f"{job_type}.py")
        if job_type == "etl":
            pulumi.Output.all(
                script_arn=self._glue_etl_job_script.arn,
                output_table=self._tables[table_path.name].name,
            ).apply(
                lambda args: self.db_manager.add_job(
                    EmrJob(
                        name=f"{self.config.project_slug}-{table_path.name}-{job_type}",
                        job_type=JobType.ETL,
                        connection_name=connection_name,
                        connection_type=ConnectionType(connection_args.connection_type),
                        connection_properties=json.dumps(connection_args.model_dump()),
                        sql_table=table_path.name,
                        incremental_column=incremental_column,
                        is_full_load=False,
                        script_id=self.db_manager.get_script_by_name(script_key).id,
                        requirements=requirements,
                        venv_s3_path=venv_s3_path,
                    ),
                    input_tables=[],
                    output_tables=[self.db_manager.get_table_by_name(table_path.name)],
                )
            )
        elif job_type == "retl":
            input_tables = get_input_tables(table_path / f"{job_type}.py")
            pulumi.Output.all(
                script_arn=self._glue_etl_job_script.arn,
                output_table=self._tables[table_path.name].name,
            ).apply(
                lambda args: self.db_manager.add_job(
                    EmrJob(
                        name=f"{self.config.project_slug}-{table_path.name}-{job_type}",
                        job_type=JobType.RETL,
                        connection_name=connection_name,
                        connection_type=ConnectionType(connection_args.connection_type),
                        connection_properties=json.dumps(connection_args.model_dump()),
                        sql_table=table_path.name,
                        incremental_column=incremental_column,
                        is_full_load=True,
                        script_id=self.db_manager.get_script_by_name(script_key).id,
                        requirements=requirements,
                        venv_s3_path=venv_s3_path,
                    ),
                    input_tables=[
                        self.db_manager.get_table_by_name(table_name)
                        for table_name in input_tables
                    ],
                    output_tables=[self.db_manager.get_table_by_name(table_path.name)],
                )
            )

    def _discover_spark_jobs(self):
        """Discover spark jobs in the data directory and setup glue jobs for them."""
        for table_path in self.config.data_dir.iterdir():
            # Check if the table path is a directory. If so, check if there's an etl.py file.
            if table_path.is_dir():
                etl_script_path = table_path / "etl.py"
                if etl_script_path.exists():
                    self._setup_glue_job(table_path, "etl")

                retl_script_path = table_path / "retl.py"
                if retl_script_path.exists():
                    self._setup_glue_job(table_path, "retl")

    def _construct_pulumi_program(self):
        """Initial program for stack creation"""
        self._ensure_base_resources()
        self._ensure_existing_tables()
        self._setup_glue()
        self._discover_spark_jobs()

    def create_stack(self):
        """Create or update the entire stack"""
        self.db_manager.reset()
        self.initialize_stack()
        up_result = self.stack.up(on_output=lambda msg: click.echo(f"Pulumi: {msg}"))
        return up_result

    def preview_stack(self):
        """Preview the stack"""
        self.initialize_stack()
        preview_result = self.stack.preview(
            on_output=lambda msg: click.echo(f"Pulumi: {msg}")
        )
        return preview_result

    def refresh_stack(self):
        """Refresh the stack"""
        self.initialize_stack()
        refresh_result = self.stack.refresh(
            on_output=lambda msg: click.echo(f"Pulumi: {msg}")
        )
        return refresh_result

    def destroy_stack(self):
        """Destroy the stack"""
        self.initialize_stack()
        destroy_result = self.stack.destroy(
            on_output=lambda msg: click.echo(f"Pulumi: {msg}")
        )
        return destroy_result

    def get_stack_outputs(self) -> StackOutputs:
        """Get stack outputs from the main thread"""
        stack_outputs = self.stack.export_stack()
        secrets_providers = stack_outputs.deployment["secrets_providers"]
        secrets_state = secrets_providers["state"]
        project_name = secrets_state["project"]
        stack_name = secrets_state["stack"]
        resources: list[dict] = stack_outputs.deployment["resources"]
        table_bucket = next(
            (
                r
                for r in resources
                if r["type"] == "aws:s3tables/tableBucket:TableBucket"
            ),
            None,
        )
        table_namespace = next(
            (r for r in resources if r["type"] == "aws:s3tables/namespace:Namespace"),
            None,
        )
        tables = [r for r in resources if r["type"] == "aws:s3tables/table:Table"]
        glue_role_arn = next(
            (r for r in resources if r["type"] == "aws:iam/role:Role"),
            None,
        )
        emr_app_id = next(
            (
                r
                for r in resources
                if r["type"] == "aws:emrserverless/application:Application"
            ),
            None,
        )
        emr_script_bucket = next(
            (
                r
                for r in resources
                if r["type"] == "aws:s3tables/tableBucket:TableBucket"
            ),
            None,
        )
        return StackOutputs(
            project_name=project_name,
            stack_name=stack_name,
            resources=resources,
            table_bucket=table_bucket,
            table_namespace=table_namespace,
            tables=tables,
            glue_role=glue_role_arn,
            emr_app=emr_app_id,
            emr_script_bucket=emr_script_bucket,
            emr_scripts=[],
            emr_jobs=[],
        )

    @classmethod
    def get_connection_info(cls) -> tuple[str, str]:
        instance = cls()
        bucket_arn = instance.get_stack_outputs().table_bucket["outputs"]["arn"]
        namespace = instance.get_stack_outputs().table_namespace["outputs"]["namespace"]
        return bucket_arn, namespace
