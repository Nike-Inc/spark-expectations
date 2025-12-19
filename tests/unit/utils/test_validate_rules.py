import pytest
import sqlglot
from unittest.mock import Mock, MagicMock

from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules
from spark_expectations.core.exceptions import SparkExpectationsInvalidRowDQExpectationException
from tests.unit.utils.conftest import (
    CHECK_AGG_DQ_VALID_AGGREGATE_FUNCTIONS,
    CHECK_AGG_DQ_CASE_INSENSITIVE,
    CHECK_AGG_DQ_LEADING_WHITESPACE,
    CHECK_AGG_DQ_WHITESPACE_BEFORE_PAREN,
    CHECK_AGG_DQ_NON_AGGREGATE_EXPRESSIONS,
    CHECK_AGG_DQ_AGGREGATE_NOT_AT_START,
    CHECK_AGG_DQ_EDGE_CASES_NEGATIVE,
    CHECK_AGG_DQ_COMPLEX_EXPRESSIONS,
    VALIDATE_SUBQUERY_VALID,
    VALIDATE_SUBQUERY_MISSING_FROM,
    VALIDATE_SUBQUERY_COMPLEX_PROJECTIONS,
    CHECK_QUERY_DQ_VALID_SELECT,
    CHECK_QUERY_DQ_NESTED_SUBQUERIES,
    CHECK_QUERY_DQ_NON_SELECT,
    CHECK_QUERY_DQ_EDGE_CASES,
    VALIDATE_ROW_DQ_VALID_EXPECTATIONS,
    VALIDATE_ROW_DQ_INVALID_AGGREGATE,
    VALIDATE_ROW_DQ_INVALID_QUERY,
    VALIDATE_ROW_DQ_INVALID_SYNTAX,
)


class TestCheckAggDq:
    """Unit tests for SparkExpectationsValidateRules.check_agg_dq"""

    @pytest.mark.parametrize(
        "expectation, expected_agg_func",
        CHECK_AGG_DQ_VALID_AGGREGATE_FUNCTIONS,
    )
    def test_check_agg_dq_with_valid_aggregate_functions(self, expectation, expected_agg_func):
        """Test that all supported aggregate functions are correctly identified."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is True
        assert matched_func.lower() == expected_agg_func.lower()

    @pytest.mark.parametrize(
        "expectation, expected_agg_func",
        CHECK_AGG_DQ_CASE_INSENSITIVE,
    )
    def test_check_agg_dq_case_insensitive(self, expectation, expected_agg_func):
        """Test that aggregate function detection is case-insensitive."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is True
        assert matched_func.lower() == expected_agg_func.lower()

    @pytest.mark.parametrize(
        "expectation, expected_agg_func",
        CHECK_AGG_DQ_LEADING_WHITESPACE,
    )
    def test_check_agg_dq_with_leading_whitespace(self, expectation, expected_agg_func):
        """Test that leading whitespace before aggregate functions is handled."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is True
        assert matched_func.lower() == expected_agg_func.lower()

    @pytest.mark.parametrize(
        "expectation, expected_agg_func",
        CHECK_AGG_DQ_WHITESPACE_BEFORE_PAREN,
    )
    def test_check_agg_dq_with_whitespace_before_parenthesis(self, expectation, expected_agg_func):
        """Test that whitespace between function name and parenthesis is handled."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is True
        assert matched_func.lower() == expected_agg_func.lower()

    @pytest.mark.parametrize(
        "expectation",
        CHECK_AGG_DQ_NON_AGGREGATE_EXPRESSIONS,
    )
    def test_check_agg_dq_with_non_aggregate_expressions(self, expectation):
        """Test that non-aggregate expressions return False."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is False
        assert matched_func is None

    @pytest.mark.parametrize(
        "expectation",
        CHECK_AGG_DQ_AGGREGATE_NOT_AT_START,
    )
    def test_check_agg_dq_aggregate_not_at_start(self, expectation):
        """Test that aggregate functions not at the start return False."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is False
        assert matched_func is None

    @pytest.mark.parametrize(
        "expectation",
        CHECK_AGG_DQ_EDGE_CASES_NEGATIVE,
    )
    def test_check_agg_dq_edge_cases_negative(self, expectation):
        """Test edge cases that should return False."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is False
        assert matched_func is None

    @pytest.mark.parametrize(
        "expectation, expected_agg_func",
        CHECK_AGG_DQ_COMPLEX_EXPRESSIONS,
    )
    def test_check_agg_dq_complex_aggregate_expressions(self, expectation, expected_agg_func):
        """Test complex aggregate expressions are correctly identified."""
        result, matched_func = SparkExpectationsValidateRules.check_agg_dq(expectation)
        assert result is True
        assert matched_func.lower() == expected_agg_func.lower()


class TestValidateSingleSubquery:
    """Unit tests for SparkExpectationsValidateRules._validate_single_subquery"""

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERY_VALID)
    def test_validate_single_subquery_valid(self, sql):
        """Test that valid subqueries pass validation without raising exceptions."""
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERY_MISSING_FROM)
    def test_validate_single_subquery_missing_from(self, sql):
        """Test that subqueries without FROM clause raise exception."""
        subquery = sqlglot.parse_one(sql)
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules._validate_single_subquery(subquery)
        assert "FROM" in str(exc_info.value)

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERY_COMPLEX_PROJECTIONS)
    def test_validate_single_subquery_complex_projections(self, sql):
        """Test that subqueries with complex projections pass validation."""
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    def test_validate_single_subquery_missing_select(self):
        """Test that non-SELECT statements inside subquery raise exception."""
        # Create a subquery that wraps a non-SELECT expression
        non_select_expr = sqlglot.parse_one("col1 > 10")
        fake_subquery = sqlglot.expressions.Subquery(this=non_select_expr)
        
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules._validate_single_subquery(fake_subquery)
        assert "SELECT" in str(exc_info.value)

    def test_validate_single_subquery_no_projections(self):
        """Test that SELECT without projections raises exception."""
        # Create a SELECT node with FROM but no projections
        select_node = sqlglot.expressions.Select(
            **{
                "from": sqlglot.expressions.From(
                    this=sqlglot.expressions.Table(this=sqlglot.expressions.Identifier(this="table1"))
                ),
                "expressions": [],  # Empty projections
            }
        )
        fake_subquery = sqlglot.expressions.Subquery(this=select_node)
        
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules._validate_single_subquery(fake_subquery)
        assert "projection" in str(exc_info.value).lower()

    def test_validate_single_subquery_with_join(self):
        """Test that subqueries with JOIN are valid."""
        sql = "(SELECT t1.col1 FROM table1 t1 INNER JOIN table2 t2 ON t1.id = t2.id)"
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    def test_validate_single_subquery_with_nested_subquery_in_from(self):
        """Test that subqueries with nested subquery in FROM are valid."""
        sql = "(SELECT a.col1 FROM (SELECT col1 FROM table1) AS a)"
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    def test_validate_single_subquery_with_window_function(self):
        """Test that subqueries with window functions are valid."""
        sql = "(SELECT col1, row_number() OVER (PARTITION BY col2 ORDER BY col3) AS rn FROM table1)"
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    def test_validate_single_subquery_with_aggregate_and_group_by(self):
        """Test that subqueries with aggregate functions and GROUP BY are valid."""
        sql = "(SELECT col1, count(*), sum(col2) FROM table1 GROUP BY col1)"
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)

    def test_validate_single_subquery_with_aliased_columns(self):
        """Test that subqueries with aliased columns are valid."""
        sql = "(SELECT col1 AS a, col2 AS b, sum(col3) AS total FROM table1 GROUP BY col1, col2)"
        subquery = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules._validate_single_subquery(subquery)


class TestCheckQueryDq:
    """Unit tests for SparkExpectationsValidateRules.check_query_dq"""

    @pytest.mark.parametrize("sql", CHECK_QUERY_DQ_VALID_SELECT)
    def test_check_query_dq_valid_select(self, sql):
        """Test that valid SELECT queries return True."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.check_query_dq(tree)
        assert result is True

    @pytest.mark.parametrize("sql", CHECK_QUERY_DQ_NESTED_SUBQUERIES)
    def test_check_query_dq_nested_subqueries(self, sql):
        """Test that nested subqueries containing SELECT return True."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.check_query_dq(tree)
        assert result is True

    @pytest.mark.parametrize("sql", CHECK_QUERY_DQ_NON_SELECT)
    def test_check_query_dq_non_select(self, sql):
        """Test that non-SELECT expressions return False."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.check_query_dq(tree)
        assert result is False

    @pytest.mark.parametrize("sql", CHECK_QUERY_DQ_EDGE_CASES)
    def test_check_query_dq_edge_cases(self, sql):
        """Test edge cases that should return False."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.check_query_dq(tree)
        assert result is False

    def test_check_query_dq_with_deeply_nested_subquery(self):
        """Test that deeply nested subqueries still return True."""
        # Create a deeply nested subquery structure
        select_node = sqlglot.parse_one("SELECT col1 FROM table1")
        # Wrap in multiple Subquery layers
        inner = sqlglot.expressions.Subquery(this=select_node)
        middle = sqlglot.expressions.Subquery(this=inner)
        outer = sqlglot.expressions.Subquery(this=middle)
        result = SparkExpectationsValidateRules.check_query_dq(outer)
        assert result is True

    def test_check_query_dq_with_complex_select(self):
        """Test complex SELECT with multiple clauses returns True."""
        sql = "(SELECT col1, sum(col2) FROM table1 WHERE col3 > 10 GROUP BY col1 HAVING sum(col2) > 100 ORDER BY col1)"
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.check_query_dq(tree)
        assert result is True


class TestValidateRowDqExpectation:
    """Unit tests for SparkExpectationsValidateRules.validate_row_dq_expectation"""

    @pytest.fixture
    def mock_df(self):
        """Create a mock DataFrame that simulates successful select/limit operations."""
        mock = MagicMock()
        # Chain mock to return itself for method chaining (df.select().limit())
        mock.select.return_value.limit.return_value = mock
        return mock

    @pytest.fixture
    def mock_df_with_error(self):
        """Create a mock DataFrame that raises an error on select."""
        mock = MagicMock()
        mock.select.side_effect = Exception("Column not found")
        return mock

    @pytest.fixture
    def mock_expr(self, mocker):
        """Mock the expr function to avoid needing Spark context."""
        return mocker.patch(
            "spark_expectations.utils.validate_rules.expr",
            return_value=MagicMock()
        )

    @pytest.mark.parametrize("rule", VALIDATE_ROW_DQ_VALID_EXPECTATIONS)
    def test_validate_row_dq_expectation_valid(self, mock_df, mock_expr, rule):
        """Test that valid row_dq expectations pass validation."""
        # Should not raise any exception
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        # Verify df.select was called with the expression
        mock_df.select.assert_called_once()

    @pytest.mark.parametrize("rule", VALIDATE_ROW_DQ_INVALID_AGGREGATE)
    def test_validate_row_dq_expectation_rejects_aggregates(self, mock_df, mock_expr, rule):
        """Test that row_dq expectations with aggregate functions are rejected."""
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        assert "aggregate" in str(exc_info.value).lower()

    @pytest.mark.parametrize("rule", VALIDATE_ROW_DQ_INVALID_QUERY)
    def test_validate_row_dq_expectation_rejects_queries(self, mock_df, mock_expr, rule):
        """Test that row_dq expectations with SELECT queries are rejected."""
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        assert "query" in str(exc_info.value).lower()

    @pytest.mark.parametrize("rule", VALIDATE_ROW_DQ_INVALID_SYNTAX)
    def test_validate_row_dq_expectation_rejects_invalid_syntax(self, mock_df, mock_expr, rule):
        """Test that row_dq expectations with invalid syntax are rejected."""
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        # Should mention parsing error or rule failed validation
        assert "row_dq" in str(exc_info.value).lower()

    def test_validate_row_dq_expectation_empty_expectation(self, mock_df, mock_expr):
        """Test that empty expectation is handled."""
        rule = {"expectation": "", "rule": "empty_rule"}
        # Empty string may parse but fail validation
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException):
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)

    def test_validate_row_dq_expectation_df_select_error(self, mock_df_with_error, mock_expr):
        """Test that DataFrame select errors are caught and wrapped."""
        rule = {"expectation": "col1 > 10", "rule": "valid_but_df_fails"}
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df_with_error, rule)
        assert "rule failed validation" in str(exc_info.value).lower()

    def test_validate_row_dq_expectation_nested_query(self, mock_df, mock_expr):
        """Test that deeply nested SELECT queries are rejected."""
        rule = {
            "expectation": "((SELECT col1 FROM table1))",
            "rule": "nested_select",
        }
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        assert "query" in str(exc_info.value).lower()

    def test_validate_row_dq_expectation_missing_expectation_key(self, mock_df, mock_expr):
        """Test handling when rule dict is missing the 'expectation' key."""
        rule = {"rule": "missing_expectation"}  # Missing 'expectation' key
        # rule.get("expectation", "") returns empty string
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException):
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)

    def test_validate_row_dq_expectation_complex_valid_expression(self, mock_df, mock_expr):
        """Test complex but valid row-level expression."""
        rule = {
            "expectation": "CASE WHEN col1 > 0 THEN col2 * 2 ELSE coalesce(col3, 0) END > 100",
            "rule": "complex_case",
        }
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        mock_df.select.assert_called_once()

    def test_validate_row_dq_expectation_with_multiple_conditions(self, mock_df, mock_expr):
        """Test row_dq with multiple AND/OR conditions."""
        rule = {
            "expectation": "(col1 > 0 AND col2 < 100) OR (col3 IS NOT NULL AND col4 = 'active')",
            "rule": "multiple_conditions",
        }
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        mock_df.select.assert_called_once()

