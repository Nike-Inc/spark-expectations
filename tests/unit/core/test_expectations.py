"""
Unit tests for spark_expectations.core.expectations module.
"""
import pytest

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter


class TestValidateRules:
    """Test cases for SparkExpectations._validate_rules method."""

    def test_validate_rules_passes_with_valid_df(self, spark, rules_df_schema, base_rule_data):
        """Test that _validate_rules passes when all required columns exist and have no NULL values."""
        rules_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        # Should not raise any exception
        se = SparkExpectations(
            product_id="test_product",
            rules_df=rules_df,
            stats_table="test_stats_table",
            stats_table_writer=writer,
            target_and_error_table_writer=writer,
        )
        assert se is not None

    # ==========================================================================
    # Empty DataFrame Tests
    # ==========================================================================

    def test_validate_rules_raises_error_when_dataframe_is_empty(self, spark, empty_rules_df):
        """Test that _validate_rules raises error when rules_df is empty."""
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=empty_rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "empty" in str(exc_info.value).lower()

    # ==========================================================================
    # Missing Columns Tests
    # ==========================================================================

    def test_validate_rules_raises_error_when_missing_single_column(
        self, spark, rules_df_schema_missing_product_id, rule_data_missing_product_id
    ):
        """Test that _validate_rules raises error when a single required column is missing."""
        rules_df = spark.createDataFrame(rule_data_missing_product_id, schema=rules_df_schema_missing_product_id)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "missing required columns" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)

    def test_validate_rules_raises_error_when_missing_multiple_columns(
        self, spark, rules_df_schema_missing_multiple, rule_data_missing_multiple
    ):
        """Test that _validate_rules raises error when multiple required columns are missing."""
        rules_df = spark.createDataFrame(rule_data_missing_multiple, schema=rules_df_schema_missing_multiple)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "missing required columns" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)
        assert "rule_type" in str(exc_info.value)

    # ==========================================================================
    # NULL Value Tests
    # ==========================================================================

    def test_validate_rules_raises_error_when_single_column_has_null(
        self, spark, rules_df_schema, rule_data_null_product_id
    ):
        """Test that _validate_rules raises error when a required column has NULL value."""
        rules_df = spark.createDataFrame(rule_data_null_product_id, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)

    def test_validate_rules_raises_error_when_multiple_columns_have_null(
        self, spark, rules_df_schema, rule_data_all_null_ids
    ):
        """Test that _validate_rules raises error when multiple required columns have NULL values."""
        rules_df = spark.createDataFrame(rule_data_all_null_ids, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        # All four required columns should be mentioned
        assert "product_id" in str(exc_info.value)
        assert "table_name" in str(exc_info.value)
        assert "rule" in str(exc_info.value)
        assert "rule_type" in str(exc_info.value)

    def test_validate_rules_raises_error_when_null_in_any_row(
        self, spark, rules_df_with_partial_null_rows
    ):
        """Test that _validate_rules raises error even if only some rows have NULL values."""
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df_with_partial_null_rows,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)

    # ==========================================================================
    # Empty String Tests
    # ==========================================================================

    def test_validate_rules_raises_error_when_column_has_empty_string(
        self, spark, rules_df_schema, rule_data_empty_string_product_id
    ):
        """Test that _validate_rules raises error when a required column has empty string value."""
        rules_df = spark.createDataFrame(rule_data_empty_string_product_id, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)

    def test_validate_rules_raises_error_when_column_has_whitespace_only(
        self, spark, rules_df_schema, rule_data_whitespace_only_product_id
    ):
        """Test that _validate_rules raises error when a required column has whitespace-only value."""
        rules_df = spark.createDataFrame(rule_data_whitespace_only_product_id, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)

    def test_validate_rules_raises_error_when_multiple_columns_have_empty_values(
        self, spark, rules_df_schema, rule_data_multiple_empty_columns
    ):
        """Test that _validate_rules raises error when multiple columns have empty/whitespace values."""
        rules_df = spark.createDataFrame(rule_data_multiple_empty_columns, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        assert "NULL or empty" in str(exc_info.value)
        assert "product_id" in str(exc_info.value)
        assert "table_name" in str(exc_info.value)
        assert "rule_type" in str(exc_info.value)

    # ==========================================================================
    # Error Message Format Tests
    # ==========================================================================

    def test_validate_rules_error_message_lists_columns_sorted(
        self, spark, rules_df_schema, rule_data_all_null_ids
    ):
        """Test that error message lists missing/null columns in sorted order."""
        rules_df = spark.createDataFrame(rule_data_all_null_ids, schema=rules_df_schema)
        writer = WrappedDataFrameWriter().mode("append").format("parquet")
        
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
            SparkExpectations(
                product_id="test_product",
                rules_df=rules_df,
                stats_table="test_stats_table",
                stats_table_writer=writer,
                target_and_error_table_writer=writer,
            )
        
        error_msg = str(exc_info.value)
        # Columns should appear in sorted order
        product_id_pos = error_msg.index("product_id")
        rule_pos = error_msg.index("'rule'")  # Use quotes to distinguish from rule_type
        rule_type_pos = error_msg.index("rule_type")
        table_name_pos = error_msg.index("table_name")
        
        assert product_id_pos < rule_pos < rule_type_pos < table_name_pos


class TestAddHashColumns:
    """Test cases for SparkExpectations._add_hash_columns method."""

    def test_add_hash_columns_adds_id_hash_column(self, spark, se_instance, rules_df_schema, base_rule_data):
        """Test that _add_hash_columns adds id_hash column to the DataFrame."""
        input_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        
        assert "id_hash" in result_df.columns

    def test_add_hash_columns_adds_expectation_hash_column(self, spark, se_instance, rules_df_schema, base_rule_data):
        """Test that _add_hash_columns adds expectation_hash column to the DataFrame."""
        input_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        
        assert "expectation_hash" in result_df.columns

    def test_add_hash_columns_produces_deterministic_hashes(self, spark, se_instance, rules_df_schema, base_rule_data):
        """Test that hash columns produce deterministic (consistent) values for same input."""
        input_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        
        result_df_1 = se_instance._add_hash_columns(input_df)
        result_df_2 = se_instance._add_hash_columns(input_df)
        
        row1 = result_df_1.collect()[0]
        row2 = result_df_2.collect()[0]
        
        assert row1["id_hash"] == row2["id_hash"]
        assert row1["expectation_hash"] == row2["expectation_hash"]

    def test_add_hash_columns_different_inputs_produce_different_id_hash(
        self, spark, se_instance, rules_df_schema, base_rule_data, rule_data_product2
    ):
        """Test that different input values produce different id_hash values."""
        input_df_1 = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        input_df_2 = spark.createDataFrame(rule_data_product2, schema=rules_df_schema)
        
        result_df_1 = se_instance._add_hash_columns(input_df_1)
        result_df_2 = se_instance._add_hash_columns(input_df_2)
        
        row1 = result_df_1.collect()[0]
        row2 = result_df_2.collect()[0]
        
        assert row1["id_hash"] != row2["id_hash"]

    def test_add_hash_columns_different_expectation_produces_different_expectation_hash(
        self, spark, se_instance, rules_df_schema, base_rule_data, rule_data_expectation_100
    ):
        """Test that different expectation values produce different expectation_hash values."""
        input_df_1 = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        input_df_2 = spark.createDataFrame(rule_data_expectation_100, schema=rules_df_schema)
        
        result_df_1 = se_instance._add_hash_columns(input_df_1)
        result_df_2 = se_instance._add_hash_columns(input_df_2)
        
        row1 = result_df_1.collect()[0]
        row2 = result_df_2.collect()[0]
        
        assert row1["expectation_hash"] != row2["expectation_hash"]

    def test_add_hash_columns_handles_null_product_id(
        self, spark, se_instance, rules_df_schema, rule_data_null_product_id
    ):
        """Test that _add_hash_columns handles null product_id gracefully via coalesce."""
        input_df = spark.createDataFrame(rule_data_null_product_id, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should not raise an error and should produce a valid hash
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32  # MD5 hash length

    def test_add_hash_columns_handles_null_table_name(
        self, spark, se_instance, rules_df_schema, rule_data_null_table_name
    ):
        """Test that _add_hash_columns handles null table_name gracefully via coalesce."""
        input_df = spark.createDataFrame(rule_data_null_table_name, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should not raise an error and should produce a valid hash
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32  # MD5 hash length

    def test_add_hash_columns_handles_null_rule(
        self, spark, se_instance, rules_df_schema, rule_data_null_rule
    ):
        """Test that _add_hash_columns handles null rule gracefully via coalesce."""
        input_df = spark.createDataFrame(rule_data_null_rule, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should not raise an error and should produce a valid hash
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32  # MD5 hash length

    def test_add_hash_columns_handles_null_rule_type(
        self, spark, se_instance, rules_df_schema, rule_data_null_rule_type
    ):
        """Test that _add_hash_columns handles null rule_type gracefully via coalesce."""
        input_df = spark.createDataFrame(rule_data_null_rule_type, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should not raise an error and should produce a valid hash
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32  # MD5 hash length

    def test_add_hash_columns_handles_all_null_id_fields(
        self, spark, se_instance, rules_df_schema, rule_data_all_null_ids
    ):
        """Test that _add_hash_columns handles all null id fields gracefully."""
        input_df = spark.createDataFrame(rule_data_all_null_ids, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should produce hash of "|||" (empty strings joined with pipes)
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32  # MD5 hash length

    def test_add_hash_columns_preserves_original_columns(self, spark, se_instance, rules_df_schema, base_rule_data):
        """Test that _add_hash_columns preserves all original columns."""
        input_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        original_columns = set(input_df.columns)
        
        result_df = se_instance._add_hash_columns(input_df)
        result_columns = set(result_df.columns)
        
        # All original columns should be preserved
        assert original_columns.issubset(result_columns)
        # Plus two new hash columns
        assert len(result_columns) == len(original_columns) + 2

    def test_add_hash_columns_multiple_rows(self, spark, se_instance, rules_df_schema, multiple_rows_data):
        """Test that _add_hash_columns works correctly with multiple rows."""
        input_df = spark.createDataFrame(multiple_rows_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        rows = result_df.collect()
        
        assert len(rows) == 2
        # Each row should have unique id_hash (different product_id, table_name, rule, rule_type)
        assert rows[0]["id_hash"] != rows[1]["id_hash"]
        # Each row should have unique expectation_hash (different expectation)
        assert rows[0]["expectation_hash"] != rows[1]["expectation_hash"]

    def test_add_hash_columns_id_hash_uses_correct_fields(
        self, spark, se_instance, rules_df_schema, same_id_different_expectation_data
    ):
        """Test that id_hash is computed from product_id, table_name, rule, and rule_type."""
        # Two records with same id fields but different expectations
        input_df = spark.createDataFrame(same_id_different_expectation_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        rows = result_df.collect()
        
        # id_hash should be the same since product_id, table_name, rule, rule_type are identical
        assert rows[0]["id_hash"] == rows[1]["id_hash"]
        # expectation_hash should be different
        assert rows[0]["expectation_hash"] != rows[1]["expectation_hash"]

    def test_add_hash_columns_expectation_hash_only_uses_expectation(
        self, spark, se_instance, rules_df_schema, different_id_same_expectation_data
    ):
        """Test that expectation_hash is computed only from the expectation field."""
        # Two records with different id fields but same expectation
        input_df = spark.createDataFrame(different_id_same_expectation_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        rows = result_df.collect()
        
        # id_hash should be different
        assert rows[0]["id_hash"] != rows[1]["id_hash"]
        # expectation_hash should be the same since expectation is identical
        assert rows[0]["expectation_hash"] == rows[1]["expectation_hash"]

    def test_add_hash_columns_hash_format_is_md5(self, spark, se_instance, rules_df_schema, base_rule_data):
        """Test that hash values are valid MD5 format (32 hex characters)."""
        input_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # MD5 hash is 32 hex characters
        assert len(row["id_hash"]) == 32
        assert len(row["expectation_hash"]) == 32
        # Should only contain hex characters
        assert all(c in "0123456789abcdef" for c in row["id_hash"])
        assert all(c in "0123456789abcdef" for c in row["expectation_hash"])

    def test_add_hash_columns_trims_whitespace_from_id_fields(
        self, spark, se_instance, rules_df_schema, base_rule_data, rule_data_with_whitespace
    ):
        """Test that whitespace is trimmed from id fields before computing hash."""
        # Create DataFrames - one with clean data, one with whitespace
        clean_df = spark.createDataFrame(base_rule_data, schema=rules_df_schema)
        whitespace_df = spark.createDataFrame(rule_data_with_whitespace, schema=rules_df_schema)
        
        # Add hash columns to both
        clean_result = se_instance._add_hash_columns(clean_df)
        whitespace_result = se_instance._add_hash_columns(whitespace_df)
        
        clean_row = clean_result.collect()[0]
        whitespace_row = whitespace_result.collect()[0]
        
        # id_hash should be identical after trimming whitespace
        assert clean_row["id_hash"] == whitespace_row["id_hash"]

    def test_add_hash_columns_trims_whitespace_produces_valid_hash(
        self, spark, se_instance, rules_df_schema, rule_data_with_whitespace
    ):
        """Test that trimmed whitespace data still produces valid MD5 hash."""
        input_df = spark.createDataFrame(rule_data_with_whitespace, schema=rules_df_schema)
        
        result_df = se_instance._add_hash_columns(input_df)
        row = result_df.collect()[0]
        
        # Should produce a valid 32-character MD5 hash
        assert row["id_hash"] is not None
        assert len(row["id_hash"]) == 32
        assert all(c in "0123456789abcdef" for c in row["id_hash"])
