### Configure Rules in the **`catalog`.`schema`.`{product}_rules`**  

Please find the data set which used for the data quality rules setup [order.csv](https://github.com/Nike-Inc/spark-expectations/blob/main/spark_expectations/examples/resources/order.csv)

##### Example Of Row, Aggregation And Query Rules For Data Quality 

To perform row data quality checks for artificially order table, please set up rules using the specified format

```sql
insert into `catalog`.`schema`.`{product}_rules` (product_id, table_name, rule_type, rule, column_name, expectation, 
action_if_failed, tag, description, enable_for_source_dq_validation,  enable_for_target_dq_validation, is_active) values
    
--The row data quality has been set on customer_id when customer_id is null, drop respective row into error table 
--as "action_if_failed" tagged "drop"
('apla_nd', '`catalog`.`schema`.customer_order',  'row_dq', 'customer_id_is_not_null', 'customer_id', 
'customer_id is not null','drop', 'validity', 'customer_id should not be null', false, false, true)

--The row data quality has been set on sales when sales is less than zero, drop respective row into error table as 
--'action_if_failed' tagged "drop"
,('apla_nd', '`catalog`.`schema`.customer_order', 'row_dq', 'sales_greater_than_zero', 'sales', 'sales > 0', 
'drop', 'accuracy', 'sales value should be greater than zero', false, false, true)

--The row data quality has been set on discount when discount is less than 60, drop respective row into error table
--and final table  as "action_if_failed" tagged 'ignore'
,('apla_nd', '`catalog`.`schema`.customer_order', 'row_dq', 'discount_threshold', 'discount', 'discount*100 < 60',
'ignore', 'validity', 'discount should be less than 40', false, false, true)

--The row data quality has been set on ship_mode when ship_mode not in ("second class", "standard class", 
--"standard class"), drop respective row into error table and fail the framework  as "action_if_failed" tagged "fail"
,('apla_nd', '`catalog`.`schema`.customer_order', 'row_dq', 'ship_mode_in_set', 'ship_mode', 'lower(trim(ship_mode))
in('second class', 'standard class', 'standard class')', 'fail', 'validity', 'ship_mode mode belongs in the sets',
false, false, true)

--The row data quality has been set on profit when profit is less than or equals to 0, drop respective row into 
--error table and final table as "action_if_failed" tagged "ignore"
,('apla_nd', '`catalog`.`schema`.customer_order', 'row_dq', 'profit_threshold', 'profit', 'profit>0', 'ignore', 
'validity', 'profit threshold should be greater than 0', false, false, true)
 
--The rule has been established to identify and remove completely identical records in which rows repeat with the 
--same value more than once, while keeping one instance of the row. Any additional duplicated rows will be dropped 
--into error table as action_if_failed set to "drop"
,('apla_nd', '`catalog`.`schema`.customer_order', 'row_dq', 'complete_duplicate', 'All', 'row_number() 
 over(partition by customer_id, order_id order by 1)=1', 'drop', 'uniqueness', 'drop complete duplicate records', 
 false, false, true)

```


Please set up rules for checking the quality of the columns in the artificial order table, using the specified format

```sql
insert into `catalog`.`schema`.`{product}_rules` (product_id, table_name, rule_type, rule, column_name, expectation, 
action_if_failed, tag, description,  enable_for_source_dq_validation,  enable_for_target_dq_validation, is_active) values
     
--The aggregation rule is established on the 'sales' column and the metadata of the rule will be captured in the 
--statistics table when the sum of the sales values falls below 10000
,('apla_nd', '`catalog`.`schema`.customer_order', 'agg_dq', 'sum_of_sales', 'sales', 'sum(sales)>10000', 'ignore', 
'validity', 'sum of sales must be greater than 10000',  true, true, true)
 
--The aggregation rule is established on the 'ship_mode' column and the metadata of the rule will be captured in 
--the statistics table when distinct ship_mode greater than 3 and enabled for only source data set
,('apla_nd', '`catalog`.`schema`.customer_order', 'agg_dq', 'distinct_of_ship_mode', 'ship_mode', 
'count(distinct ship_mode)<=3', 'ignore', 'validity', 'regex format validation for quantity', true, false, true)

-- The aggregation rule is established on the table count and the metadata of the rule will be captured in the 
--statistics table when distinct count greater than 10000 and fails the job as "action_if_failed" set to "fail" 
--and enabled only for validated dataset
,('apla_nd', '`catalog`.`schema`..customer_order', 'agg_dq', 'row_count', '*', 'count(*)>=10000', 'fail', 'validity',
'distinct ship_mode must be less or equals to 3', false, true, true)

```

Please set up rules for checking the quality of artificially order table by implementing query data quality option, using the specified format

```sql
insert into `catalog`.`schema`.`{product}_rules` (product_id, table_name, rule_type, rule, column_name, expectation, 
action_if_failed, tag, description) values

--The query dq rule is established to check product_id difference between two table if difference is more than 20% 
--from source table, the metadata of the rule will be captured in the statistics table as "action_if_failed" is "ignore"
,('apla_nd', '`catalog`.`schema`.customer_order', 'query_dq', 'product_missing_count_threshold', '*', 
'((select count(distinct product_id) from {table}) - (select count(distinct product_id) from order))>
(select count(distinct product_id) from product)*0.2', 'ignore', 'validity', 'row count threshold difference must 
be less than 20%', true, true, true)
 
--The query dq rule is established to check distinct product_id in the product table is less than 5, if not the 
--metadata of the rule will be captured in the statistics table along with fails the job as "action_if_failed" is 
--"fail" and enabled for source dataset
,('apla_nd', '`catalog`.`schema`.customer_order', 'query_dq', 'product_category', '*', '(select count(distinct category) 
from {table}) < 5', 'fail', 'validity', 'distinct product category must be less than 5', true, False, true)

--The query dq rule is established to check count of the dataset should be less than 10000 other wise the metadata 
--of the rule will be captured in the statistics table as "action_if_failed" is "ignore" and enabled only for target dataset
,('apla_nd', '`catalog`.`schema`.customer_order', 'query_dq', 'row_count_in_order', '*', 
'(select count(*) from order)<10000', 'ignore', 'accuracy', 'count of the row in order dataset must be less then 10000', 
false, true, true)

```