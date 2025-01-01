from pathlib import Path
import importlib.util

from nextdata.core.glue.connections.generic_connection import (
    GenericConnectionGlueJobArgs,
)


def has_custom_glue_job(file_path: Path) -> bool:
    if file_path.exists():
        # Check if the etl.py file has a @glue_job decorator by importing and inspecting
        spec = importlib.util.spec_from_file_location("etl_module", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            # Look for any function decorated with @glue_job
            if callable(attr) and hasattr(attr, "__wrapped__"):
                # Check if this function was decorated by glue_job
                if attr.__wrapped__.__name__ == "glue_job_wrapper":
                    return True
    return False


def get_connection_name(file_path: Path) -> str:
    spec = importlib.util.spec_from_file_location("etl_module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    connection_name = getattr(module, "connection_name", None)
    return connection_name


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
    spec = importlib.util.spec_from_file_location("etl_module", file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, "incremental_column", "created_at")
