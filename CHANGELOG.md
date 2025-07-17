# Changelog
----------------------------
## v2.5 (2025-07-25)
----------------------------
### Features
- `stats_table` schema has been modified to include an additional column "column_name" ([#168](https://github.com/Nike-Inc/spark-expectations/pull/168))
- Swap out Poetry for hatch for package management, and introduce matrix texting for Python 3.10 - 3.12. ([#146](https://github.com/Nike-Inc/spark-expectations/pull/146))
    - 3.9 has been excluded from testing.
- Added jinja email templates for basic email notications from SE ([#165](https://github.com/Nike-Inc/spark-expectations/pull/165))
### Fixes
- Resetting `row_dq_error_threshold` prior to each SE run (previous values would pre-exist into next SE call) ([#166](https://github.com/Nike-Inc/spark-expectations/pull/166))
- Fixed regex matching issue for agg_dq when using count(*) with range ([#171](https://github.com/Nike-Inc/spark-expectations/pull/171))
- Fixed defect to enable agg rules for Strings and Date column types ([#158](https://github.com/Nike-Inc/spark-expectations/pull/158))
- Change the path to use pkf_resource like employee dataset ([#156](https://github.com/Nike-Inc/spark-expectations/pull/156))
- Fixed issue with orders.csv dataset getting loaded from examples ([#164](https://github.com/Nike-Inc/spark-expectations/pull/164))