import pytest
from unittest.mock import Mock, patch, MagicMock, call
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.context import SparkContext
from pyspark.sql.column import Column

# Create a single mock for SparkManager that will be used throughout
mock_spark_manager = MagicMock()
mock_spark_session = MagicMock(spec=SparkSession)
mock_reader = MagicMock()
mock_jdbc = MagicMock()
mock_df = MagicMock(spec=DataFrame)

# Configure the mock chain
mock_df.count.return_value = 10
mock_df.show.return_value = None
mock_df.withColumn.return_value = mock_df
mock_jdbc.return_value = mock_df
mock_reader.jdbc = mock_jdbc
mock_spark_session.read = mock_reader
mock_spark_manager.spark = mock_spark_session

# Create mock DSQL classes and functions at module level
mock_dsql_args_class = MagicMock()
mock_jdbc_args_class = MagicMock()
mock_generate_password_func = MagicMock()

# Patch SparkManager and other dependencies
with patch(
    "nextdata.core.connections.spark.SparkManager", return_value=mock_spark_manager
), patch(
    "nextdata.core.glue.connections.dsql.DSQLGlueJobArgs", mock_dsql_args_class
), patch(
    "nextdata.core.glue.connections.jdbc.JDBCGlueJobArgs", mock_jdbc_args_class
), patch(
    "nextdata.core.glue.connections.dsql.generate_dsql_password",
    mock_generate_password_func,
), patch(
    "pyspark.sql.functions.current_date"
) as mock_current_date:
    mock_current_date.return_value = Mock(spec=Column)
    from nextdata.core.glue.default_etl_script import main


@pytest.fixture(autouse=True)
def mock_spark_context():
    with patch("pyspark.SparkContext") as mock_sc:
        sc = mock_sc.return_value
        SparkContext._active_spark_context = sc
        yield sc
        SparkContext._active_spark_context = None


@patch("argparse.ArgumentParser.parse_args")
def test_main_dsql_connection(mock_parse_args):
    # Reset mock call counts
    mock_jdbc.reset_mock()
    mock_spark_manager.write_to_table.reset_mock()
    mock_dsql_args_class.reset_mock()
    mock_generate_password_func.reset_mock()

    # Mock DSQL configuration
    mock_dsql_config = MagicMock()
    mock_dsql_config.protocol = "dsql"
    mock_dsql_config.host = "test-host"
    mock_dsql_config.port = "5439"
    mock_dsql_config.database = "test_db"
    mock_dsql_config.username = "test_user"
    mock_dsql_args_class.return_value = mock_dsql_config
    mock_generate_password_func.return_value = "test_password"

    # Mock command line arguments
    mock_args = {
        "job_name": "test_etl",
        "connection_name": "test_conn",
        "connection_type": "dsql",
        "connection_properties": json.dumps(
            {"host": "test-host", "port": "5439", "database": "test_db"}
        ),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
        "is_full_load": "true",
    }
    mock_parse_args.return_value = Mock(**mock_args)

    # Run the main function
    main(spark_manager=mock_spark_manager)

    # Print debug info about mock calls
    print(f"DSQLGlueJobArgs mock calls: {mock_dsql_args_class.mock_calls}")
    print(
        f"generate_dsql_password mock calls: {mock_generate_password_func.mock_calls}"
    )

    # Verify DSQL configuration was created correctly
    mock_dsql_args_class.assert_called_once_with(host="test-host")
    mock_generate_password_func.assert_called_once_with(mock_dsql_config.host)

    # Get the call arguments without accessing the Java properties
    assert mock_jdbc.called, "JDBC read was not called"
    call_args = mock_jdbc.call_args[1]

    # Verify the URL and table name
    assert "jdbc:dsql://" in call_args["url"]
    assert "test-host" in call_args["url"]
    assert call_args["table"] == "test_table"

    # Verify the properties dictionary directly
    props = call_args["properties"]
    assert isinstance(props, dict)
    assert props["user"] == "test_user"
    assert props["password"] == "test_password"
    assert props["ssl"] == True
    assert props["sslmode"] == "require"

    # Verify write_to_table was called
    assert mock_spark_manager.write_to_table.called, "write_to_table was not called"
    write_args = mock_spark_manager.write_to_table.call_args[1]
    assert write_args["table_name"] == "test_table"
    assert write_args["mode"] == "overwrite"  # since is_full_load is True


@patch("argparse.ArgumentParser.parse_args")
def test_main_jdbc_connection(mock_parse_args):
    # Reset mock call counts
    mock_jdbc.reset_mock()
    mock_spark_manager.write_to_table.reset_mock()

    # Mock JDBC configuration
    mock_jdbc_config = MagicMock()
    mock_jdbc_config.protocol = "postgresql"
    mock_jdbc_config.host = "test-host"
    mock_jdbc_config.port = "5432"
    mock_jdbc_config.database = "test_db"
    mock_jdbc_config.username = "test_user"
    mock_jdbc_config.password = "test_pass"
    mock_jdbc_args_class.return_value = mock_jdbc_config

    # Mock command line arguments for JDBC connection
    mock_args = {
        "job_name": "test_etl",
        "connection_name": "test_conn",
        "connection_type": "jdbc",
        "protocol": "postgresql",
        "connection_properties": json.dumps(
            {
                "protocol": "postgresql",
                "host": "test-host",
                "port": "5432",
                "database": "test_db",
                "username": "test_user",
                "password": "test_pass",
            }
        ),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
        "is_full_load": "false",
    }
    mock_parse_args.return_value = Mock(**mock_args)

    # Run the main function
    main(spark_manager=mock_spark_manager)

    # Verify JDBC configuration was created correctly
    mock_jdbc_args_class.assert_called_once_with(
        **json.loads(mock_args["connection_properties"])
    )

    # Get the call arguments without accessing the Java properties
    assert mock_jdbc.called, "JDBC read was not called"
    call_args = mock_jdbc.call_args[1]

    # Verify the URL and table name
    assert "jdbc:postgresql://" in call_args["url"]
    assert call_args["table"] == "test_table"

    # Verify the properties dictionary directly
    props = call_args["properties"]
    assert isinstance(props, dict)
    assert props["user"] == "test_user"
    assert props["password"] == "test_pass"
    assert props["ssl"] == True
    assert props["sslmode"] == "require"

    # Verify write_to_table was called
    assert mock_spark_manager.write_to_table.called, "write_to_table was not called"
    write_args = mock_spark_manager.write_to_table.call_args[1]
    assert write_args["table_name"] == "test_table"
    assert write_args["mode"] == "append"  # since is_full_load is False


@patch("argparse.ArgumentParser.parse_args")
def test_main_unsupported_connection(mock_parse_args):
    # Reset mock call counts
    mock_jdbc.reset_mock()
    mock_spark_manager.write_to_table.reset_mock()

    # Mock command line arguments with unsupported connection type
    mock_args = {
        "job_name": "test_etl",
        "connection_name": "test_conn",
        "connection_type": "unsupported",
        "connection_properties": json.dumps({"host": "test-host"}),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
    }
    mock_parse_args.return_value = Mock(**mock_args)

    # Verify that unsupported connection type raises ValueError
    with pytest.raises(ValueError, match="1 validation error"):
        main(spark_manager=mock_spark_manager)
