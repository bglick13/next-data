import click
import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
from pathlib import Path

from nextdata.cli.types import StackOutputs

from .project_config import NextDataConfig


class PulumiContextManager:
    def __init__(self):
        self.config = NextDataConfig.from_env()
        self._stack = None
        self._table_bucket = None
        self._table_namespace = None
        self._tables = {}  # Keep track of tables by name

    @property
    def stack(self):
        if not self._stack:
            self.initialize_stack()
        return self._stack

    @property
    def table_bucket(self):
        if not self._table_bucket:
            self.initialize_stack()
        return self._table_bucket

    @property
    def table_namespace(self):
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

    def _ensure_base_resources(self):
        """Ensure bucket and namespace exist"""
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
        for table_path in self.config.data_dir.iterdir():
            if table_path.is_dir():
                table_name = table_path.name
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

    def _construct_pulumi_program(self):
        """Initial program for stack creation"""
        self._ensure_base_resources()
        self._ensure_existing_tables()

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
    def get_connection_info(cls) -> StackOutputs:
        instance = cls()
        bucket_arn = instance.get_stack_outputs().table_bucket["outputs"]["arn"]
        namespace = instance.get_stack_outputs().table_namespace["outputs"]["namespace"]
        return bucket_arn, namespace
