def get_s3_table_path(namespace: str, table_name: str) -> str:
    """
    Get the fully qualified s3 table path
    """
    return f"s3tablesbucket.{namespace}.{table_name}"
