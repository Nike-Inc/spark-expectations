import pytest
import sqlglot
from unittest.mock import MagicMock

from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules
from spark_expectations.core.exceptions import SparkExpectationsInvalidRowDQExpectationException
from tests.unit.utils.conftest import (
    VALIDATE_SUBQUERY_VALID,
    VALIDATE_SUBQUERY_MISSING_FROM,
    VALIDATE_SUBQUERY_COMPLEX_PROJECTIONS,
    VALIDATE_SUBQUERY_MISSING_PROJECTIONS,
    CHECK_QUERY_DQ_VALID_SELECT,
    CHECK_QUERY_DQ_NESTED_SUBQUERIES,
    CHECK_QUERY_DQ_NON_SELECT,
    CHECK_QUERY_DQ_EDGE_CASES,
    VALIDATE_ROW_DQ_VALID_EXPECTATIONS,
    VALIDATE_ROW_DQ_INVALID_AGGREGATE,
    VALIDATE_ROW_DQ_INVALID_QUERY,
    VALIDATE_ROW_DQ_INVALID_SYNTAX,
    VALIDATE_SUBQUERIES_ONE_VALID,
    VALIDATE_SUBQUERIES_NESTED_VALID,
    VALIDATE_SUBQUERIES_INVALID,
    GET_SUBQUERIES_NONE,
    GET_SUBQUERIES_ONE,
    GET_SUBQUERIES_MULTIPLE,
    GET_SUBQUERIES_NESTED,
    CHECK_AGG_OUTSIDE_SUBQUERIES_TRUE,
    CHECK_AGG_OUTSIDE_SUBQUERIES_FALSE,
)

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

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERY_MISSING_PROJECTIONS)
    def test_validate_single_subquery_missing_projections(self, sql):
        """Test that subqueries without projections raise exception."""
        subquery = sqlglot.parse_one(sql)
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules._validate_single_subquery(subquery)
        assert "Subquery does not contain any valid projections" in str(exc_info.value)

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


class TestValidateSubqueries:
    """Unit tests for SparkExpectationsValidateRules.validate_subqueries"""

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERIES_ONE_VALID)
    def test_validate_subqueries_one_valid(self, sql):
        """Test that expressions with one valid subquery pass validation."""
        tree = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules.validate_subqueries(tree)

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERIES_NESTED_VALID)
    def test_validate_subqueries_nested_valid(self, sql):
        """Test that expressions with nested valid subqueries pass validation."""
        tree = sqlglot.parse_one(sql)
        # Should not raise any exception
        SparkExpectationsValidateRules.validate_subqueries(tree)

    @pytest.mark.parametrize("sql", VALIDATE_SUBQUERIES_INVALID)
    def test_validate_subqueries_invalid(self, sql):
        """Test that expressions with invalid subqueries raise exception."""
        tree = sqlglot.parse_one(sql)
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_subqueries(tree)
        assert "subquery" in str(exc_info.value).lower()

    def test_validate_subqueries_finds_all_subqueries(self):
        """Test that validate_subqueries validates ALL subqueries in expression."""
        # Expression with 3 subqueries - all valid
        sql = "(SELECT a FROM t1) + (SELECT b FROM t2) + (SELECT c FROM t3)"
        tree = sqlglot.parse_one(sql)
        # Should not raise - all subqueries are valid
        SparkExpectationsValidateRules.validate_subqueries(tree)

    def test_validate_subqueries_exception_contains_subquery_info(self):
        """Test that exception message contains information about the invalid subquery."""
        sql = "col1 IN (SELECT id)"  # Invalid - missing FROM
        tree = sqlglot.parse_one(sql)
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_subqueries(tree)
        error_msg = str(exc_info.value)
        assert "FROM" in error_msg or "subquery" in error_msg.lower()


class TestGetSubqueries:
    """Unit tests for SparkExpectationsValidateRules.get_subqueries"""

    @pytest.mark.parametrize("sql", GET_SUBQUERIES_NONE)
    def test_get_subqueries_returns_empty_list_when_no_subqueries(self, sql):
        """Test that expressions with no subqueries return an empty list."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.get_subqueries(tree)
        assert result == []
        assert isinstance(result, list)

    @pytest.mark.parametrize("sql,expected_count", GET_SUBQUERIES_ONE)
    def test_get_subqueries_returns_one_subquery(self, sql, expected_count):
        """Test that expressions with one subquery return a list with one element."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.get_subqueries(tree)
        assert len(result) == expected_count

    @pytest.mark.parametrize("sql,expected_count", GET_SUBQUERIES_MULTIPLE)
    def test_get_subqueries_returns_multiple_subqueries(self, sql, expected_count):
        """Test that expressions with multiple subqueries return correct count."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.get_subqueries(tree)
        assert len(result) == expected_count

    @pytest.mark.parametrize("sql,expected_count", GET_SUBQUERIES_NESTED)
    def test_get_subqueries_finds_nested_subqueries(self, sql, expected_count):
        """Test that nested subqueries are all found and counted."""
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.get_subqueries(tree)
        assert len(result) == expected_count

    def test_get_subqueries_with_complex_expression(self):
        """Test get_subqueries with complex expression containing multiple subquery types."""
        sql = """
            CASE 
                WHEN col1 IN (SELECT id FROM t1) THEN 1
                WHEN col2 > (SELECT max(val) FROM t2) THEN 2
                ELSE 0
            END
        """
        tree = sqlglot.parse_one(sql)
        result = SparkExpectationsValidateRules.get_subqueries(tree)
        assert len(result) == 2

    def test_get_subqueries_result_can_be_used_as_boolean(self):
        """Test that result can be used in boolean context (for bool() conversion)."""
        # With subqueries - should be truthy
        tree_with = sqlglot.parse_one("col1 IN (SELECT id FROM t1)")
        result_with = SparkExpectationsValidateRules.get_subqueries(tree_with)
        assert bool(result_with) is True

        # Without subqueries - should be falsy
        tree_without = sqlglot.parse_one("col1 > 10")
        result_without = SparkExpectationsValidateRules.get_subqueries(tree_without)
        assert bool(result_without) is False


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

    def test_validate_row_dq_expectation_calls_validate_subqueries(self, mock_df, mock_expr, mocker):
        """Test that validate_subqueries is called when expectation contains a subquery."""
        mock_validate_subqueries = mocker.patch.object(
            SparkExpectationsValidateRules,
            "validate_subqueries"
        )
        rule = {
            "expectation": "col1 IN (SELECT id FROM table1)",
            "rule": "subquery_rule",
        }
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        # Verify validate_subqueries was called
        mock_validate_subqueries.assert_called_once()

    def test_validate_row_dq_expectation_subquery_with_aggregate_inside_passes(self, mock_df, mock_expr):
        """Test that subquery with aggregate function inside passes validation."""
        rule = {
            "expectation": "col1 > (SELECT sum(col2) FROM table1)",
            "rule": "agg_inside_subquery",
        }
        # Should not raise - aggregate is inside subquery
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        mock_df.select.assert_called_once()

    def test_validate_row_dq_expectation_subquery_with_aggregate_outside_fails(self, mock_df, mock_expr):
        """Test that aggregate function outside subquery raises exception."""
        rule = {
            "expectation": "sum(col1) > (SELECT max(col2) FROM table1)",
            "rule": "agg_outside_subquery",
        }
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException) as exc_info:
            SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        assert "outside of subquery" in str(exc_info.value).lower()

    def test_validate_row_dq_expectation_multiple_aggregates_all_inside_subqueries_passes(self, mock_df, mock_expr):
        """Test that multiple aggregates all inside subqueries passes validation."""
        rule = {
            "expectation": "col1 IN (SELECT max(id) FROM t1) AND col2 > (SELECT avg(val) FROM t2)",
            "rule": "multi_agg_inside_subqueries",
        }
        # Should not raise - all aggregates are inside subqueries
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        mock_df.select.assert_called_once()

    def test_validate_row_dq_expectation_calls_check_agg_outside_subqueries(self, mock_df, mock_expr, mocker):
        """Test that check_agg_outside_subqueries is called when subquery and aggregates exist."""
        mock_check_agg = mocker.patch.object(
            SparkExpectationsValidateRules,
            "check_agg_outside_subqueries",
            return_value=False  # All aggregates inside subqueries
        )
        rule = {
            "expectation": "col1 > (SELECT sum(col2) FROM table1)",
            "rule": "subquery_with_agg",
        }
        SparkExpectationsValidateRules.validate_row_dq_expectation(mock_df, rule)
        # Verify check_agg_outside_subqueries was called
        mock_check_agg.assert_called_once()


class TestCheckAggOutsideSubqueries:
    """Unit tests for SparkExpectationsValidateRules.check_agg_outside_subqueries"""

    @pytest.mark.parametrize("sql,agg_funcs", CHECK_AGG_OUTSIDE_SUBQUERIES_TRUE)
    def test_check_agg_outside_subqueries_returns_true(self, sql, agg_funcs):
        """Test that expressions with aggregates outside subqueries return True."""
        tree = sqlglot.parse_one(sql)
        # Get actual aggregate functions from the tree
        actual_agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
        result = SparkExpectationsValidateRules.check_agg_outside_subqueries(tree, actual_agg_funcs)
        assert result is True

    @pytest.mark.parametrize("sql,agg_funcs", CHECK_AGG_OUTSIDE_SUBQUERIES_FALSE)
    def test_check_agg_outside_subqueries_returns_false(self, sql, agg_funcs):
        """Test that expressions with all aggregates inside subqueries return False."""
        tree = sqlglot.parse_one(sql)
        # Get actual aggregate functions from the tree
        actual_agg_funcs = list({node.key for node in tree.find_all(sqlglot.expressions.AggFunc)})
        result = SparkExpectationsValidateRules.check_agg_outside_subqueries(tree, actual_agg_funcs)
        assert result is False
