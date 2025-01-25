from pathlib import Path
import importlib.util

from nextdata.core.data.data_table import DataTable
from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)
from nextdata.util.ndx_ast import NextDataVisitor


def has_custom_glue_job(file_path: Path) -> bool:
    if not file_path.exists():
        return False

    visitor = NextDataVisitor(file_path)
    visitor.visit(visitor.tree)
    return visitor.has_glue_job


def get_connection_name(file_path: Path) -> str:
    visitor = NextDataVisitor(file_path)
    visitor.visit(visitor.tree)
    return visitor.connection_name


def get_connection_args(
    connection_name: str, connections_dir: Path
) -> type[GenericConnectionGlueJobArgs]:
    connection_path = connections_dir / connection_name / "main.py"
    connection_spec = importlib.util.spec_from_file_location(
        f"connection_{connection_name}", connection_path
    )
    connection_module = importlib.util.module_from_spec(connection_spec)
    connection_spec.loader.exec_module(connection_module)
    connection_args = None
    # find the attr that inherits from GenericConnectionGlueJobArgs
    for attr_name in dir(connection_module):
        attr = getattr(connection_module, attr_name)
        if isinstance(attr, GenericConnectionGlueJobArgs):
            connection_args = attr
            break
        elif isinstance(attr, type) and issubclass(attr, GenericConnectionGlueJobArgs):
            # Find instance of this class in the module
            for instance_name in dir(connection_module):
                instance = getattr(connection_module, instance_name)
                if isinstance(instance, attr):
                    config_instance = instance
                    break
            if config_instance and isinstance(config_instance, attr):
                connection_args = config_instance
                break
    if not connection_args:
        raise ValueError(
            f"No connection arguments found in {connection_path}. Please add a connection_args variable that inherits from GenericConnectionGlueJobArgs."
        )
    return connection_args


def get_incremental_column(file_path: Path) -> str:
    visitor = NextDataVisitor(file_path)
    visitor.visit(visitor.tree)
    return visitor.incremental_column or "created_at"


def get_input_tables(file_path: Path) -> list[str]:
    visitor = NextDataVisitor(file_path)
    visitor.visit(visitor.tree)
    return visitor.input_tables
