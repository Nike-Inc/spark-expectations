# Welcome to Spark-Expectations

Taking inspiration from DLT - data quality expectations: Spark-Expectations is built, so that the data quality rules can 
run using decorator pattern while the spark job is in flight and Additionally, the framework able to perform data 
quality checks when the data is at rest.

### Features Of Spark Expectations

Please find the spark-expectations flow and feature diagrams below

<p align="center">
<img src=https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/flow.png?raw=true width=1000></p>

<p align="center">
<img src=https://github.com/Nike-Inc/spark-expectations/blob/main/docs/se_diagrams/features.png?raw=true width=1000></p>


## Concept
Most of the data quality tools do the data quality checks or data validation on a table at rest and provide metrics in 
different forms. `While the existing tools are good to do profiling and provide metrics, below are the problems that we 
commonly see` 

* The existing tools do not perform any action or remove the malformed data in the original table 
* Most existing frameworks do not offer the capability to perform both row and column level data quality checks 
within a single tool.
* User have to manually check the provided metrics, and it becomes cumbersome to find the records which doesn't meet 
the data quality standards
* Downstream users have to consume the same data with error, or they have to do additional computation to remove the 
records that doesn't meet the standards
* Another process is required as a corrective action to rectify the errors in the data and lot of planning is usually 
required for this activity

`Spark-Expectations solves all of the above problems by following the below principles`

* Spark Expectations provides the ability to run both individual row-based and overall aggregated data quality rules 
on both the source and validated data sets. In case a rules fails, the row-level error is recorded in the `_error` table 
and a summarized report of all failed aggregated data quality rules is compiled in the `_stats` table
* All the records which fail one or more data quality rules, are by default quarantined in an `_error` table along with 
the metadata on rules that failed, job information etc. This helps analysts or products to look at the error data easily 
and work with the teams required to correct the data and reprocess it easily
* Aggregated Metrics are provided on the job level along with necessary metadata so that recalculation or compute is 
avoided
* The data that doesn't meet the data quality contract or the standards is not written into the final table unless or
otherwise specified. 
* By default, frameworks have the capability to send notifications only upon failure, but they have the ability to 
send notifications at the start, as well as upon completion


There is a field in the rules table called [action_if_failed](getting-started/setup/#action_if_failed), which determines
what needs to be done if a rule fails


* Let's consider a hypothetical scenario, where we have 100 columns and with 200
row level data quality rules, 10 aggregation data quality rules and 5 query data quality rules  computed against. When the dq job is run, there are
10 rules that failed on a particular row and 4 aggregation rules fails- what determines if that row should end up in 
final table or not? Below are the hierarchy of checks that happens?
* Among the row level 10 rules failed, if there is at least one rule which has an _action_if_failed_ as _fail_ - 
  then the job will be failed 
  * Among the 10 row level rules failed, if there is no rule that has an _action_if_failed_ as _fail_, but at least 
  has one rule with _action_if_failed_ as _drop_ - then the record/row will be dropped
  * Among the 10 row level rules failed, if no rule neither has _fail_ nor _drop_ as an _action_if_failed_ - then 
  the record will be end up in the final table. Note that, this record would also exist in the `_error` table
  * The aggregation and query dq rules have a setting called `action_if_failed` with two options: `fail` or `ignore`. If any of
  the 10 aggregation rules and 5 query dq rules which failed has an _action_if_failed_as_fail_, then the metadata summary will be 
  recorded in the `_stats` table and the job will be considered a failure. However, if none of the failed rules 
  has an _action_if_failed_as_fail_, then summary of the aggregated rules' metadata will still be collected in the 
  `_stats` table for failed aggregated and  query dq rules.
