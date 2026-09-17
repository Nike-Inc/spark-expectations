"""Unit tests for ``spark_expectations.sinks.utils.stats_metadata``.

These tests verify that both Kafka transports produce byte-identical on-topic
payloads for the ``se_job_metadata`` column. The helper is the single source of
truth for that transformation; regressions here would silently change the shape
of published metric events for either transport.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StructType

from spark_expectations.core import get_spark_session
from spark_expectations.sinks.utils.stats_metadata import apply_se_job_metadata_struct


spark = get_spark_session()


def test_apply_se_job_metadata_struct_returns_mock_unchanged_when_column_absent():
    fake_df = MagicMock()
    result = apply_se_job_metadata_struct(fake_df)
    assert result is fake_df
    # `.select` must NOT have been called — the column-absent short-circuit
    # protects the pre-existing MagicMock-based REST writer tests.
    fake_df.select.assert_not_called()


def test_apply_se_job_metadata_struct_returns_df_unchanged_when_column_missing_on_real_df():
    df = spark.createDataFrame([("p1",)], schema=StructType([StructField("product_id", StringType(), True)]))
    result = apply_se_job_metadata_struct(df)
    assert result is df


def test_apply_se_job_metadata_struct_noop_when_sample_is_none():
    df = spark.createDataFrame(
        [("p1", None)],
        schema=StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("se_job_metadata", StringType(), True),
            ]
        ),
    )
    result = apply_se_job_metadata_struct(df)
    # Column is preserved as a StringType (no schema_of_json("") derivation).
    assert result.schema["se_job_metadata"].dataType.simpleString() == "string"


def test_apply_se_job_metadata_struct_noop_when_sample_is_empty_string():
    df = spark.createDataFrame(
        [("p1", "")],
        schema=StructType(
            [
                StructField("product_id", StringType(), True),
                StructField("se_job_metadata", StringType(), True),
            ]
        ),
    )
    result = apply_se_job_metadata_struct(df)
    assert result.schema["se_job_metadata"].dataType.simpleString() == "string"


def test_apply_se_job_metadata_struct_converts_to_nested_object_in_to_json_output():
    metadata_dict = {"se_version": "1.0.0", "runtime_env": {"host": "local"}}
    df = spark.createDataFrame([("p1",)], schema=StructType([StructField("product_id", StringType(), True)]))
    df = df.withColumn("se_job_metadata", lit(json.dumps(metadata_dict)))

    # Precondition: metadata is a JSON string.
    assert df.schema["se_job_metadata"].dataType.simpleString() == "string"

    converted = apply_se_job_metadata_struct(df)

    # It is now a struct.
    assert converted.schema["se_job_metadata"].dataType.simpleString() != "string"

    # And downstream ``to_json(struct(*))`` — the shared serialisation used by
    # both transports — renders it as a nested object rather than a
    # double-escaped string.
    payload = json.loads(converted.selectExpr("to_json(struct(*)) AS value").first()[0])
    assert isinstance(payload["se_job_metadata"], dict)
    assert payload["se_job_metadata"]["se_version"] == "1.0.0"
    assert payload["se_job_metadata"]["runtime_env"]["host"] == "local"
