from pydantic import ValidationError
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from nextdata.core.glue.glue_entrypoint import GlueJobArgs, glue_job
from nextdata.core.connections.spark import SparkManager


def test_glue_job_args_validation():
    # Test valid arguments
    valid_args = {
        "job_name": "test_job",
        "connection_name": "test_conn",
        "connection_type": "dsql",
        "connection_properties": json.dumps({"host": "test-host"}),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
    }

    args = GlueJobArgs(**valid_args)

    assert args.job_name == "test_job"
    assert args.connection_properties == {"host": "test-host"}
    assert args.is_full_load == True  # Default value

    # Test invalid connection type
    with pytest.raises(ValueError):
        invalid_args = valid_args.copy()
        invalid_args["connection_type"] = "invalid"
        GlueJobArgs(**invalid_args)

    # Test invalid JSON in connection properties
    with pytest.raises(ValidationError):
        invalid_args = valid_args.copy()
        invalid_args["connection_properties"] = "invalid json"
        GlueJobArgs(**invalid_args)


@patch("argparse.ArgumentParser.parse_args")
def test_glue_job_decorator(mock_parse_args):
    # Mock the arguments that would be passed via command line
    mock_args = {
        "job_name": "test_job",
        "connection_name": "test_conn",
        "connection_type": "dsql",
        "connection_properties": json.dumps({"host": "test-host"}),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
    }
    mock_parse_args.return_value = Mock(**mock_args)

    # Create a mock function to decorate
    mock_result = "success"

    # Create a mock SparkManager class that will be used by the decorator
    mock_spark_manager = MagicMock(spec=SparkManager)
    with patch(
        "nextdata.core.glue.glue_entrypoint.SparkManager",
        return_value=mock_spark_manager,
    ):

        @glue_job()
        def test_func(spark_manager, job_args):
            assert isinstance(spark_manager, SparkManager)
            assert isinstance(job_args, GlueJobArgs)
            assert job_args.job_name == "test_job"
            assert job_args.connection_properties == {"host": "test-host"}
            return mock_result

        # Call the decorated function
        result = test_func()
        assert result == mock_result


@patch("argparse.ArgumentParser.parse_args")
def test_glue_job_decorator_with_custom_args(mock_parse_args):
    # Define custom job args
    class CustomJobArgs(GlueJobArgs):
        custom_field: str

    # Mock the arguments
    mock_args = {
        "job_name": "test_job",
        "connection_name": "test_conn",
        "connection_type": "dsql",
        "connection_properties": json.dumps({"host": "test-host"}),
        "sql_table": "test_table",
        "bucket_arn": "arn:aws:s3:::test-bucket",
        "namespace": "test",
        "custom_field": "custom_value",
    }
    mock_parse_args.return_value = Mock(**mock_args)

    # Create a mock SparkManager class that will be used by the decorator
    mock_spark_manager = MagicMock(spec=SparkManager)
    with patch(
        "nextdata.core.glue.glue_entrypoint.SparkManager",
        return_value=mock_spark_manager,
    ):

        @glue_job(JobArgsType=CustomJobArgs)
        def test_func(spark_manager, job_args):
            assert isinstance(spark_manager, SparkManager)
            assert isinstance(job_args, CustomJobArgs)
            assert job_args.custom_field == "custom_value"
            return "success"

        result = test_func()
        assert result == "success"
