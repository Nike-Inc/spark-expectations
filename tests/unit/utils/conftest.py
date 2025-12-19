"""
Conftest file containing test parameters for validate_rules unit tests.
"""

# ==================== check_agg_dq Test Parameters ====================

# Valid aggregate functions - each tuple is (expectation, expected_agg_func)
CHECK_AGG_DQ_VALID_AGGREGATE_FUNCTIONS = [
    ("sum(col1) > 10", "sum"),
    ("count(col1) > 5", "count"),
    ("avg(col1) < 100", "avg"),
    ("mean(col1) >= 50", "mean"),
    ("min(col1) > 0", "min"),
    ("max(col1) < 1000", "max"),
    ("stddev(col1) > 1", "stddev"),
    ("stddev_samp(col1) > 1", "stddev_samp"),
    ("stddev_pop(col1) > 1", "stddev_pop"),
    ("variance(col1) > 10", "variance"),
    ("var_samp(col1) > 10", "var_samp"),
    ("var_pop(col1) > 10", "var_pop"),
    ("collect_list(col1) is not null", "collect_list"),
    ("collect_set(col1) is not null", "collect_set"),
]

# Case insensitive aggregate functions - each tuple is (expectation, expected_agg_func)
CHECK_AGG_DQ_CASE_INSENSITIVE = [
    ("SUM(col1) > 10", "SUM"),
    ("Sum(col1) > 10", "Sum"),
    ("COUNT(col1) > 5", "COUNT"),
    ("AVG(col1) < 100", "AVG"),
    ("MEAN(col1) >= 50", "MEAN"),
    ("MIN(col1) > 0", "MIN"),
    ("MAX(col1) < 1000", "MAX"),
    ("STDDEV(col1) > 1", "STDDEV"),
    ("STDDEV_SAMP(col1) > 1", "STDDEV_SAMP"),
    ("STDDEV_POP(col1) > 1", "STDDEV_POP"),
    ("VARIANCE(col1) > 10", "VARIANCE"),
    ("VAR_SAMP(col1) > 10", "VAR_SAMP"),
    ("VAR_POP(col1) > 10", "VAR_POP"),
    ("COLLECT_LIST(col1) is not null", "COLLECT_LIST"),
    ("COLLECT_SET(col1) is not null", "COLLECT_SET"),
]

# Leading whitespace - each tuple is (expectation, expected_agg_func)
CHECK_AGG_DQ_LEADING_WHITESPACE = [
    ("  sum(col1) > 10", "sum"),
    ("\tcount(col1) > 5", "count"),
    ("   avg(col1) < 100", "avg"),
    ("\n\tsum(col1) > 10", "sum"),
]

# Whitespace between function name and parenthesis - each tuple is (expectation, expected_agg_func)
CHECK_AGG_DQ_WHITESPACE_BEFORE_PAREN = [
    ("sum (col1) > 10", "sum"),
    ("count  (col1) > 5", "count"),
    ("avg\t(col1) < 100", "avg"),
]

# Non-aggregate expressions (should return False)
CHECK_AGG_DQ_NON_AGGREGATE_EXPRESSIONS = [
    "col1 > 10",
    "col1 is null",
    "col1 is not null",
    "col1 between 1 and 10",
    "col1 in (1, 2, 3)",
    "col1 like '%test%'",
    "upper(col1) = 'TEST'",
    "lower(col1) = 'test'",
    "length(col1) > 5",
    "trim(col1) = 'test'",
    "concat(col1, col2) = 'test'",
    "substring(col1, 1, 3) = 'abc'",
    "coalesce(col1, col2) is not null",
    "case when col1 > 10 then 1 else 0 end",
    "col1 + col2 > 100",
    "(col1 % 2) = 0",
]

# Aggregate not at start (should return False)
CHECK_AGG_DQ_AGGREGATE_NOT_AT_START = [
    "col1 + sum(col2) > 100",
    "(sum(col1)) > 10",  # Parenthesis before aggregate
    "1 + count(col1) > 5",
    "col1 = avg(col2)",
    "not sum(col1) > 10",
]

# Edge cases that should return False
CHECK_AGG_DQ_EDGE_CASES_NEGATIVE = [
    "",                          # Empty string
    "   ",                       # Whitespace only
    "sumcol1 > 10",              # No parenthesis (not a function call)
    "sum_col1 > 10",             # Underscore makes it a column name
    "summary(col1) > 10",        # Similar but not an aggregate
    "counter(col1) > 10",        # Similar to count but different
    "avgerage(col1) > 10",       # Typo
]

# Complex aggregate expressions - each tuple is (expectation, expected_agg_func)
CHECK_AGG_DQ_COMPLEX_EXPRESSIONS = [
    ("sum(col1) > 10 and sum(col2) > 20", "sum"),
    ("count(distinct col1) > 5", "count"),
    ("avg(col1 + col2) < 100", "avg"),
    ("sum(case when col1 > 0 then col1 else 0 end) > 10", "sum"),
    ("count(*) > 100", "count"),
    ("sum(col1) between 10 and 100", "sum"),
]

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
VALIDATE_SUBQUERY_MISSING_FROM = [
    "(SELECT col1)",
    "(SELECT col1, col2)",
    
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
    # String functions
    {"expectation": "length(col1) > 5", "rule": "col1_length"},
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
