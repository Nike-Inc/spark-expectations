"""
Conftest file containing test parameters for validate_rules unit tests.
"""

# ==================== _validate_single_subquery Test Parameters ====================

# Valid subqueries - SQL strings that should pass validation
VALIDATE_SUBQUERY_VALID = [
    # Basic column projection
    "(SELECT col1 FROM table1)",
    # Multiple columns
    "(SELECT col1, col2, col3 FROM table1)",
    # With aggregate function
    "(SELECT sum(col1) FROM table1)",
    # With alias
    "(SELECT col1 AS alias1 FROM table1)",
    # With window function
    "(SELECT col1, rank() OVER (PARTITION BY col2 ORDER BY col3) FROM table1)",
    # With nested subquery in FROM
    "(SELECT a.col1 FROM (SELECT col1 FROM table1) a)",
    # With JOIN
    "(SELECT t1.col1, t2.col2 FROM table1 t1 LEFT JOIN table2 t2 ON t1.id = t2.id)",
    # With WHERE clause
    "(SELECT col1 FROM table1 WHERE col2 > 10)",
    # With GROUP BY
    "(SELECT col1, count(*) FROM table1 GROUP BY col1)",
    # With HAVING
    "(SELECT col1, sum(col2) FROM table1 GROUP BY col1 HAVING sum(col2) > 100)",
    # With ORDER BY
    "(SELECT col1 FROM table1 ORDER BY col1 DESC)",
    # With DISTINCT
    "(SELECT DISTINCT col1 FROM table1)",
    # With CASE expression as projection
    "(SELECT CASE WHEN col1 > 0 THEN 'positive' ELSE 'negative' END FROM table1)",
    # With arithmetic expression
    "(SELECT col1 + col2 FROM table1)",
    # With function call
    "(SELECT coalesce(col1, col2) FROM table1)",
    # Complex nested
    "(SELECT col1 FROM (SELECT col1 FROM (SELECT col1 FROM table1)))",
]

# Invalid subqueries - missing SELECT (wraps non-SELECT statement)
VALIDATE_SUBQUERY_MISSING_SELECT = [
    # These are expressions that when parsed as subqueries don't have SELECT
    # Note: sqlglot may interpret some of these differently, we need valid subquery syntax
    # that doesn't contain a SELECT inside
]

# Invalid subqueries - missing FROM clause
VALIDATE_SUBQUERY_INVALID_FROM = [
    "(SELECT col1)",
    "(SELECT col1, col2)",
    'SELECT x FROM (VALUES (1), (2), (3)) AS t(x)'
]

# Invalid subqueries - empty projections (no columns selected)
# Note: These are hard to create as valid SQL - "SELECT FROM table" is invalid syntax
# sqlglot may handle these differently

# Valid subqueries with complex projections
VALIDATE_SUBQUERY_COMPLEX_PROJECTIONS = [
    # Nested alias chains
    "(SELECT sum(col1) AS total FROM table1)",
    # Multiple aggregate functions
    "(SELECT count(*), sum(col1), avg(col2) FROM table1)",
    # Window functions with aliases
    "(SELECT row_number() OVER (ORDER BY col1) AS rn FROM table1)",
    # Mixed projections
    "(SELECT col1, sum(col2) AS total, rank() OVER (ORDER BY col1) AS rnk FROM table1)",
]

VALIDATE_SUBQUERY_MISSING_PROJECTIONS = [
    "(SELECT FROM table1)"
]

# ==================== check_query_dq Test Parameters ====================

# Valid query_dq expressions - SQL that contains SELECT (should return True)
CHECK_QUERY_DQ_VALID_SELECT = [
    # Basic SELECT queries wrapped in parentheses (parsed as Subquery/Paren)
    "(SELECT col1, col2 FROM table1)",
    # With aggregate
    "(SELECT count(*) FROM table1)",
    # With WHERE
    "(SELECT col1 FROM table1 WHERE col2 > 10)",
    # With GROUP BY
    "(SELECT col1, count(*) FROM table1 GROUP BY col1)",
    # With ORDER BY
    "(SELECT col1 FROM table1 ORDER BY col1)",
    # With HAVING
    "(SELECT col1, sum(col2) FROM table1 GROUP BY col1 HAVING sum(col2) > 100)",
    # With DISTINCT
    "(SELECT DISTINCT col1 FROM table1)",
    # With JOIN
    "(SELECT t1.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id)",
]

# Nested subqueries - should still return True
CHECK_QUERY_DQ_NESTED_SUBQUERIES = [
    # Double nested
    "((SELECT col1 FROM table1))",
    # Triple nested
    "(((SELECT col1 FROM table1)))",
    # Subquery in FROM (outer is still SELECT)
    "(SELECT a.col1 FROM (SELECT col1 FROM table1) a)",
]

# Non-SELECT expressions (should return False)
CHECK_QUERY_DQ_NON_SELECT = [
    # Simple column comparisons
    "col1 > 10",
    # Aggregate expressions (not wrapped in SELECT)
    "sum(col1) > 10",
    # Boolean expressions
    "col1 > 10 AND col2 < 20",
    # Arithmetic expressions
    "col1 + col2",
    # Function calls (not SELECT)
    "upper(col1)",
    # CASE expressions
    "CASE WHEN col1 > 0 THEN 1 ELSE 0 END",
    # IN expressions
    "col1 IN (1, 2, 3)",
    # BETWEEN
    "col1 BETWEEN 1 AND 10",
    # LIKE
    "col1 LIKE '%test%'",
]

# Edge cases
CHECK_QUERY_DQ_EDGE_CASES = [
    # Parenthesized non-SELECT (should return False)
    "(col1 > 10)",
    "(sum(col1) > 10)",
    # Complex but not SELECT
    "(col1 + col2 > 100)",
]

# ==================== validate_row_dq_expectation Test Parameters ====================

# Valid row_dq expectations (should pass validation)
VALIDATE_ROW_DQ_VALID_EXPECTATIONS = [
    # Simple column comparisons
    {"expectation": "col1 between 1 and 100", "rule": "col1_between"},
    # Subqueries
    {"expectation": "col1 > (SELECT count(*) FROM table1)", "rule": "col1_from_subquery"},
    # Arithmetic expressions
    {"expectation": "col1 + col2 > 100", "rule": "col1_plus_col2"},
    # Boolean expressions
    {"expectation": "col1 > 0 AND col2 < 100", "rule": "col1_and_col2"},
    # CASE expressions
    {"expectation": "CASE WHEN col1 > 0 THEN true ELSE false END", "rule": "col1_case"},
    # Coalesce and null handling
    {"expectation": "coalesce(col1, 0) > 0", "rule": "col1_coalesce"},
    # Regex
    {"expectation": "col1 rlike '^[A-Z]+$'", "rule": "col1_regex"},
]

# Invalid row_dq expectations - contain aggregate functions (should raise exception)
VALIDATE_ROW_DQ_INVALID_AGGREGATE = [
    {"expectation": "sum(col1) > 10", "rule": "sum_col1"}
]

# Invalid row_dq expectations - contain SELECT query (should raise exception)
VALIDATE_ROW_DQ_INVALID_QUERY = [
    {"expectation": "(SELECT sum(col1) FROM table1 WHERE col2 > 10)", "rule": "select_sum"},
]

# Invalid row_dq expectations - invalid syntax (should raise exception)
VALIDATE_ROW_DQ_INVALID_SYNTAX = [
    {"expectation": "col1 = = = invalid", "rule": "invalid_syntax"},
    {"expectation": "col1 >>> 10", "rule": "invalid_operator"},
]

# ==================== validate_subqueries Test Parameters ====================

# Expressions with ONE valid subquery (should pass)
VALIDATE_SUBQUERIES_ONE_VALID = [
    # IN clause with subquery
    "col1 IN (SELECT id FROM table1)"
]

# Expressions with NESTED subqueries (subquery inside subquery - should pass)
VALIDATE_SUBQUERIES_NESTED_VALID = [
    # Subquery in FROM of another subquery
    "col1 IN (SELECT a.id FROM (SELECT id FROM table1) a)",
    # Deeply nested
    "col1 = (SELECT max(val) FROM (SELECT val FROM (SELECT val FROM table1)))",
]

# Expressions with INVALID subqueries (should raise exception)
VALIDATE_SUBQUERIES_INVALID = [
    # Subquery without FROM clause
    "col1 IN (SELECT col1)",
    # Multiple subqueries where one is invalid (missing FROM)
    "col1 IN (SELECT id FROM table1) AND col2 > (SELECT max(val))",
]

# ==================== check_agg_outside_subqueries Test Parameters ====================

# Expressions where aggregates are OUTSIDE subqueries (should return True)
CHECK_AGG_OUTSIDE_SUBQUERIES_TRUE = [
    # Aggregate with no subquery at all
    ("sum(col1) > 10", ["sum"]),
    # Aggregate outside with unrelated subquery
    ("sum(col1) > 10 AND col2 IN (SELECT id FROM table1)", ["sum"]),
    # Multiple aggregates outside, none in subqueries
    ("sum(col1) + count(col2) > 100", ["sum", "count"]),
    # Aggregate both inside and outside subquery (more outside than inside)
    ("sum(col1) > (SELECT max(col2) FROM table1) AND count(col3) > 10", ["sum", "count", "max"]),
]

# Expressions where all aggregates are INSIDE subqueries (should return False)
CHECK_AGG_OUTSIDE_SUBQUERIES_FALSE = [
    # Single subquery with aggregate
    ("col1 > (SELECT sum(col2) FROM table1)", ["sum"]),
    # IN clause with aggregate in subquery
    ("col1 IN (SELECT max(id) FROM table1)", ["max"]),
    # Nested subquery with aggregate
    ("col1 = (SELECT max(val) FROM (SELECT val FROM table1))", ["max"]),
]

# ==================== get_subqueries Test Parameters ====================

# Expressions with NO subqueries (should return empty list)
GET_SUBQUERIES_NONE = [
    # Simple column comparisons
    "col1 > 10",
    # Function calls without subquery
    "upper(col1) = 'TEST'",
    # IN with literal values (no subquery)
    "col1 IN (1, 2, 3)",
]

# Expressions with ONE subquery (should return list with 1 element)
GET_SUBQUERIES_ONE = [
    # IN clause with subquery
    ("col1 IN (SELECT id FROM table1)", 1),
    # Comparison with scalar subquery
    ("col1 > (SELECT max(val) FROM table1)", 1),
    # EXISTS
    ("EXISTS (SELECT 1 FROM table1)", 1),
]

# Expressions with MULTIPLE subqueries (should return list with expected count)
GET_SUBQUERIES_MULTIPLE = [
    # Two subqueries in comparison
    ("(SELECT count(*) FROM table1) = (SELECT count(*) FROM table2)", 2),
    # Three subqueries in arithmetic
    ("(SELECT a FROM t1) + (SELECT b FROM t2) + (SELECT c FROM t3)", 3),
    # Subqueries in AND condition
    ("col1 IN (SELECT id FROM t1) AND col2 IN (SELECT id FROM t2)", 2),
]

# Expressions with NESTED subqueries (count includes all levels)
GET_SUBQUERIES_NESTED = [
    # Subquery in FROM - outer + inner = 2
    ("col1 IN (SELECT a.id FROM (SELECT id FROM table1) a)", 2),
    # Triple nested subqueries = 3
    ("col1 = (SELECT max(val) FROM (SELECT val FROM (SELECT val FROM t1)))", 3),
]
