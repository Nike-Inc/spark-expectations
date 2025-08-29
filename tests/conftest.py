import pytest

@pytest.fixture(name="_fixture_agg_dq_rule")
def fixture_agg_dq_rule():
    # Define the expectations for the data quality rules
    return {
        "rule_type": "agg_dq",
        "rule": "col1_sum_gt_eq_6",
        "column_name": "col1",
        "expectation": "sum(col1)>=6",
        "action_if_failed": "ignore",
        "table_name": "test_table",
        "tag": "validity",
        "enable_for_source_dq_validation": True,
        "description": "col1 sum gt 1",
        "product_id": "product_1",
    }