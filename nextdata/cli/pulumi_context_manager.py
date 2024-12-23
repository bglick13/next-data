import click
import pulumi
import pulumi_aws as aws
from pulumi import automation as auto
from pathlib import Path

from .project_config import NextDataConfig


class PulumiContextManager:
    def __init__(self):
        self.config = NextDataConfig.from_env()
        self.stack = None
        self.table_bucket = None
        self.table_namespace = None
        self.tables = {}  # Keep track of tables by name

    def initialize_stack(self):
        """Initialize or get existing stack"""
        if not self.stack:
            self.stack = auto.create_or_select_stack(
                stack_name=self.config.stack_name,
                project_name=self.config.project_name.lower().replace("-", "_"),
                program=self.construct_pulumi_program,
            )
            self.stack.workspace.install_plugin("aws", "v6.66.0")
            self.stack.set_config(
                "aws:region", auto.ConfigValue(self.config.aws_region)
            )

    def ensure_base_resources(self):
        """Ensure bucket and namespace exist"""
        if not self.table_bucket:
            bucket_name = f"{self.config.project_slug}tables"
            self.table_bucket = aws.s3tables.TableBucket(
                bucket_name,
                name=bucket_name,
            )

        if not self.table_namespace:
            namespace_name = f"{self.config.project_slug}namespace"
            self.table_namespace = aws.s3tables.Namespace(
                namespace_name,
                namespace=namespace_name,
                table_bucket_arn=self.table_bucket.arn,
            )

    def create_table(self, table_path: str):
        """Create a single table and update the stack"""
        table_name = Path(table_path).name
        # Convert any non-alphanumeric characters to underscores
        safe_name = "".join(c if c.isalnum() else "_" for c in table_name.lower())

        click.echo(f"Creating table for {table_name}")

        def add_table_program():
            self.ensure_base_resources()

            # Create the new table
            table = aws.s3tables.Table(
                safe_name,
                name=safe_name,  # Use safe name for both resource and table name
                table_bucket_arn=self.table_bucket.arn,
                namespace=self.table_namespace.namespace.apply(
                    lambda ns: ns.replace("-", "_")
                ),
                format="ICEBERG",
            )

            # Export the table location
            pulumi.export(f"table_{safe_name}", table.warehouse_location)

            # Store the table reference
            self.tables[safe_name] = table

        try:
            self.initialize_stack()
            # Update the stack with the new table
            up_result = self.stack.up(
                program=add_table_program,
                on_output=lambda msg: click.echo(f"Pulumi: {msg}"),
            )
            click.echo(f"Successfully created table: {table_name}")
            return up_result
        except Exception as e:
            click.echo(f"Error creating table: {str(e)}")
            raise

    def construct_pulumi_program(self):
        """Initial program for stack creation"""
        self.ensure_base_resources()

        # Create tables for existing directories
        for table_path in self.config.data_dir.iterdir():
            if table_path.is_dir():
                table_name = table_path.name
                # Convert any non-alphanumeric characters to underscores
                safe_name = "".join(
                    c if c.isalnum() else "_" for c in table_name.lower()
                )

                if safe_name not in self.tables:
                    table = aws.s3tables.Table(
                        safe_name,
                        name=safe_name,  # Use safe name for both resource and table name
                        table_bucket_arn=self.table_bucket.arn,
                        namespace=self.table_namespace.namespace.apply(
                            lambda ns: ns.replace("-", "_")
                        ),
                        format="ICEBERG",
                    )
                    self.tables[safe_name] = table
                    pulumi.export(f"table_{safe_name}", table.warehouse_location)

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
