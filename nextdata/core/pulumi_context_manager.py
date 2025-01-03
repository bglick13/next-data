import json
from typing import Literal
import click
import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
from pathlib import Path

from nextdata.cli.types import StackOutputs
import importlib.util
import importlib.resources

from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)
from nextdata.util.framework_magic import (
    get_connection_args,
    get_connection_name,
    get_incremental_column,
    has_custom_glue_job,
)
from nextdata.util.s3_tables_utils import get_s3_table_path

from .project_config import NextDataConfig

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
        self._stack = None
        self._table_bucket = None
        self._table_namespace = None
        self._tables = {}  # Keep track of tables by name
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
        glue_role = aws.iam.Role(
            "glue-role",
            assume_role_policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "glue.amazonaws.com"},
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
            policy_arn="arn:aws:iam::aws:policy/service-role/AWSLakeFormationDataAdmin",
        )
        self._iam_role = glue_role
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

    def _setup_glue(self):
        # Create a glue catalog database and set it up for AWS analytics service integration
        glue_catalog_database = aws.glue.CatalogDatabase(
            "glue-catalog-database",
            name=f"{self.config.project_slug}database",
            target_database=aws.glue.CatalogDatabaseTargetDatabaseArgs(
                catalog_id=f"{self.table_bucket.owner_account_id}:s3tablescatalog/{self.table_bucket.name}",
                database_name=self.table_namespace.namespace.apply(
                    lambda ns: ns.replace("-", "_")
                ),
            ),
            create_table_default_permissions=[],
        )
        # Create a bucket for Glue jobs
        glue_job_bucket = aws.s3.BucketV2(
            "glue-job-bucket",
            force_destroy=True,
        )
        # Upload a Glue job script.
        # default_etl_script.py is a package module in the ndx-etl package
        glue_etl_job_script = aws.s3.BucketObject(
            "glue-etl-job-script.py",
            bucket=glue_job_bucket.id,
            source=pulumi.asset.FileAsset(
                importlib.resources.files("nextdata")
                / "core"
                / "glue"
                / "default_etl_script.py"
            ),
        )
        self._glue_catalog_database = glue_catalog_database
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

        if not self._table_namespace:
            namespace_name = f"{self.config.project_slug}namespace"
            self._table_namespace = aws.s3tables.Namespace(
                namespace_name,
                namespace=namespace_name,
                table_bucket_arn=self._table_bucket.arn,
            )

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

        # Export the table location
        pulumi.export(f"table_{safe_name}", table.warehouse_location)

        # Store the table reference
        self._tables[safe_name] = table
        click.echo(f"Creating table for {table_name}")

    def _setup_lakeformation(self):
        """Grant lakeformation permissions to the principal so analytics integration works"""
        # Set up the data lake admin
        lakeformation_settings = aws.lakeformation.DataLakeSettings(
            "lakeformation-settings",
            admins=[self.iam_role.arn],
        )

        # Grant table permissions for each table
        for table_name, table in self._tables.items():
            table_permissions = aws.lakeformation.Permissions(
                f"lakeformation-permissions-{table_name}",
                principal=self.iam_role.arn,
                table=aws.lakeformation.PermissionsTableArgs(
                    database_name=self._table_namespace.namespace,
                    name=table_name,
                    catalog_id=pulumi.Output.concat(
                        self.table_bucket.owner_account_id,
                        ":s3tablescatalog/",
                        self.table_bucket.name,
                    ),
                ),
                permissions=["ALL"],
                opts=pulumi.ResourceOptions(depends_on=[lakeformation_settings]),
            )

    def _setup_glue_job(self, table_path: Path, job_type: Literal["etl", "retl"]):
        """Setup a glue job for a table"""
        # Check if there's a custom etl script for this table by looking for an etl.py file with a @glue_job decorator

        if has_custom_glue_job(table_path / f"{job_type}.py"):
            etl_script_path = table_path / f"{job_type}.py"
            aws.s3.BucketObject(
                f"glue-etl-job-script-{table_path.name}.py",
                aws.s3.BucketObjectArgs(
                    bucket=self.glue_job_bucket.id,
                    source=pulumi.asset.FileAsset(etl_script_path),
                ),
            )
            script_location = f"s3://{self.glue_job_bucket.bucket.apply(lambda name: name)}/{table_path.name}"
        else:
            script_location = "default_etl_script.py"

        # Get the connection name from the etl.py file by checking connection_name variable
        connection_name = get_connection_name(table_path / f"{job_type}.py")
        if (
            not connection_name
            or connection_name not in self.config.get_available_connections()
        ):
            raise ValueError(
                f"No connection name found in {etl_script_path}. Please add a connection_name variable and ensure it's defined in the connections directory."
            )
        connection_args = get_connection_args(
            connection_name, self.config.connections_dir
        )
        print(f"Connection args: {connection_args}")
        incremental_column = get_incremental_column(table_path / f"{job_type}.py")
        glue_job = aws.glue.Job(
            f"glue-job-{table_path.name}",
            name=f"{self.config.project_slug}-{table_path.name}-{job_type}",
            role_arn=self.iam_role.arn,
            glue_version="3.0",
            connections=[connection_name],
            number_of_workers=1,
            worker_type="G.1X",
            timeout=3600,
            max_retries=0,
            default_arguments={
                "--JOB_NAME": self.glue_job_bucket.bucket.apply(
                    lambda name: f"{name}-{table_path.name}-{job_type}"
                ),
                "--JobType": "GLUE_ETL",
                "--ConnectionName": connection_name,
                "--ConnectionType": connection_args.connection_type,
                "--ConnectionProperties": json.dumps(connection_args.model_dump()),
                "--GlueDBName": self.glue_catalog_database.name,
                "--TempDir": self.glue_job_bucket.bucket.apply(
                    lambda name: f"s3://{name}/glue-job-temp/"
                ),
                "--SQLTable": table_path.name,
                "--OutputS3Path": get_s3_table_path(
                    self._table_namespace.namespace.apply(
                        lambda ns: ns.replace("-", "_")
                    ),
                    table_path,
                ),
                "--IncrementalColumn": incremental_column,
                "--IsFullLoad": "false",
                "--job-bookmarks-option": "job-bookmark-enable",
            },
            command=aws.glue.JobCommandArgs(
                name="glue-etl-job-script",
                script_location=self.glue_etl_job_script.bucket.apply(
                    lambda name: f"s3://{name}/{script_location}"
                ),
            ),
        )

    def _discover_etl_scripts(self):
        """Discover etl scripts in the data directory and setup glue jobs for them."""
        for table_path in self.config.data_dir.iterdir():
            # Check if the table path is a directory. If so, check if there's an etl.py file.
            if table_path.is_dir():
                etl_script_path = table_path / "etl.py"
                if etl_script_path.exists():
                    self._setup_glue_job(table_path, "etl")

    def _construct_pulumi_program(self):
        """Initial program for stack creation"""
        self._ensure_base_resources()
        self._ensure_existing_tables()
        self._setup_glue()
        self._setup_lakeformation()
        self._discover_etl_scripts()

    def create_stack(self):
        """Create or update the entire stack"""
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
        return StackOutputs(
            project_name=project_name,
            stack_name=stack_name,
            resources=resources,
            table_bucket=table_bucket,
            table_namespace=table_namespace,
            tables=tables,
        )

    @classmethod
    def get_connection_info(cls) -> tuple[str, str]:
        instance = cls()
        bucket_arn = instance.get_stack_outputs().table_bucket["outputs"]["arn"]
        namespace = instance.get_stack_outputs().table_namespace["outputs"]["namespace"]
        return bucket_arn, namespace
