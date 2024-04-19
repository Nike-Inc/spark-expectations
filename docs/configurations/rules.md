### Different Types of Expectations

Please find the different types of possible expectations 

#### Possible Row Data Quality Expectations

| rule_description |                     category                     | tag | rule_expectation |
| :------------------|:------------------------------------------------:| :-----: | ------------------: |
| Expect that the values in the column should not be null/empty |                 null_validation                  | completeness | ```[col_name] is not null``` |
| Ensure that the primary key values are unique and not duplicated  |              primary_key_validation              | uniqueness | ```count(*) over(partition by [primary_key_or_combination_of_primary_key] order by 1)=1 ```|
| Perform a thorough check to make sure that there are no duplicate values, if there are duplicates preserve one row into target |          complete_duplicate_validation           | uniqueness | ```row_number() over(partition by [all_the_column_in_dataset_b_ comma_separated] order by 1)=1```|
| Verify that the date values are in the correct format |              date_format_validation              |validity |```to_date([date_col_name], '[mention_expected_date_format]') is not null``` |
| Verify that the date values are in the correct format using regex |        date_format_validation_with_regex         | validity | ```[date_col_name] rlike '[regex_format_of_date]'``` |
| Expect column value is date parseable |    expect_column_values_to_be_date_parseable     | validity | ```try_cast([date_col_name] as date)``` | 
| Verify values in a column to conform to a specified regular expression pattern |       expect_column_values_to_match_regex        | validity | ```[col_name]  rlike '[regex_format]'``` |
| Verify values in a column to not conform to a specified regular expression pattern |     expect_column_values_to_not_match_regex      | validity | ```[col_name] not rlike '[regex_format]'``` |
| Verify values in a column to match regex in list |     expect_column_values_to_match_regex_list     | validity |  ```[col_name] not rlike '[regex format1]' or [col_name] not rlike '[regex_format2]' or [col_name] not rlike '[regex_format3]'``` |
| Expect the values in a column to belong to a specified set |        expect_column_values_to_be_in_set         | accuracy | ```[col_name] in ([values_in_comma_separated])```|
| Expect the values in a column not to belong to a specified set|      expect_column_values_to_be_not_in_set       |accuracy | ```[col_name] not in ([values_in_comma_separated])``` |
| Expect the values in a column to fall within a defined range |       expect_column_values_to_be_in_range        | accuracy | ```[col_name] between [min_threshold] and [max_threshold]``` |
| Expect the lengths of the values in a column to be within a specified range|    expect_column_value_lengths_to_be_between     | accuracy | ```length([col_name]) between [min_threshold] and [max_threshold]``` |
| Expect the lengths of the values in a column to be equal to a certain value |     expect_column_value_lengths_to_be_equal      | accuracy | ```length([col_name])=[threshold]``` |
| Expect values in the column to exceed a certain limit |      expect_column_value_to_be_greater_than      | accuracy| ```[col_name] > [threshold_value]``` |
| Expect values in the column  not to exceed a certain limit|      expect_column_value_to_be_lesser_than       | accuracy | ```[col_name] < [threshold_value]``` |
| Expect values in the column to be equal to or exceed a certain limit |      expect_column_value_greater_than_equal      | accuracy | ```[col_name] >= [threshold_value]``` |
| Expect values in the column to be equal to or not exceed a certain limit |      expect_column_value_lesser_than_equal       | accuracy | ```[col_name] <= [threshold_value]``` |
| Expect values in column A to be greater than values in column B | expect_column_pair_values_A_to_be_greater_than_B | accuracy | ```[col_A] > [col_B]``` |
| Expect values in column A to be lesser than values in column B | expect_column_pair_values_A_to_be_lesser_than_B  | accuracy | ```[col_A] < [col_B]``` |
| Expect values in column A to be greater than or equals to values in column B |       expect_column_A_to_be_greater_than_B       | accuracy | ```[col_A] >= [col_B]``` |
| Expect values in column A to be lesser than or equals to values in column B |  expect_column_A_to_be_lesser_than_or_equals_B   |accuracy  | ```[col_A] <= [col_B]``` |  
| Expect the sum of values across multiple columns to be equal to a certain value |         expect_multicolumn_sum_to_equal          | accuracy | ```[col_1] + [col_2] + [col_3] = [threshold_value]``` |
| Expect sum of values in each category equals certain value |       expect_sum_of_value_in_subset_equal        | accuracy | ```sum([col_name]) over(partition by [category_col] order by 1)``` |
| Expect count of values in each category equals certain value |      expect_count_of_value_in_subset_equal       | accuracy | ```count(*) over(partition by [category_col] order by 1)``` |
| Expect distinct value in each category exceeds certain range |     expect_distinct_value_in_subset_exceeds      | accuracy | ```count(distinct [col_name]) over(partition by [category_col] order by 1)``` |



#### Possible Aggregation Data Quality Expectations

| rule_description | rule_type | tag | rule_expectation |
|------------------|-----------|-----|------------------|
| Expect distinct values in a column that are present in a given list | expect_column_distinct_values_to_be_in_set | accuracy | ```array_intersect(collect_list(distinct [col_name]), Array($compare_values_string)) = Array($compare_values_string)``` | 
| Expect the mean value of a column to fall within a specified range| expect_column_mean_to_be_between| consistency | ```avg([col_name]) between [lower_bound] and [upper_bound]``` |
| Expect the median value of a column to be within a certain range| expect_column_median_to_be_between | consistency | ```percentile_approx([column_name], 0.5) between [lower_bound] and [upper_bound]``` |
| Expect the standard deviation of a column's values to fall within a specified range | expect_column_stdev_to_be_between | consistency | ```stddev([col_name]) between [lower_bound] and [upper_bound]``` |
| Expect the count of unique values in a column to fall within a specified range | expect_column_unique_value_count_to_be_between | accuracy | ```count(distinct [col_name]) between [lower_bound] and [upper_bound]``` |
| Expect the maximum value in a column to fall within a specified range| expect_column_max_to_be_between |accuracy |```max([col_name]) between [lower_bound] and [upper_bound]``` |
| Expect the minimum value in a column fall within a specified range | expect_column_sum_to_be_between |accuracy  | ```min([col_name]) between [lower_bound] and [upper_bound]``` |
| Expect row count of the dataset fall within certain range | expect_row_count_to_be_between | accuracy | ```count(*) between [lower_bound] and [upper_bound]``` |
| Expect row count of the dataset fall within certain range | expect_row_count_to_be_in_range | accuracy | ```count(*) >[lower_bound] and count(*) < [upper_bound]``` |


#### Possible Query Data Quality Expectations



| rule_description | rule_type | tag  | rule_expectation  |
|------------------|-----------|-----|------------------|
| Expect distinct values in a column must be greater than threshold value | expect_column_distinct_values_greater_than_threshold_value | accuracy | ```(select count(distinct [col_name]) from [table_name]) > [threshold_value]``` | 
| Expect count between two table or view must be same| expect_count_between_two_table_same | consistency | ```(select count(*) from [table_a]) = (select count(*) from [table_b])``` |
| Expect the median value of a column to be within a certain range| expect_column_median_to_be_between | consistency | ```(select percentile_approx([column_name], 0.5) from [table_name]) between [lower_bound] and [upper_bound]``` |
| Expect the standard deviation of a column's values to fall within a specified range | expect_column_stdev_to_be_between | consistency | ```(select stddev([col_name]) from [table_name]) between [lower_bound] and [upper_bound]``` |
| Expect the count of unique values in a column to fall within a specified range | expect_column_unique_value_count_to_be_between | accuracy | ```(select count(distinct [col_name]) from [table_name]) between [lower_bound] and [upper_bound]``` |
| Expect the maximum value in a column to fall within a specified range| expect_column_max_to_be_between |accuracy |```(select max([col_name]) from [table_name]) between [lower_bound] and [upper_bound]``` |
| Expect the minimum value in a column fall within a specified range | expect_column_min_to_be_between |accuracy  | ```(select min([col_name]) from [table_name]) between [lower_bound] and [upper_bound]``` |
| Expect referential integrity  | expect_referential_integrity_between_two_table_should_be_less_than_100 | accuracy |  ```( select * from [table_a] left join [table_b] on [condition] where [table_b.column] is null) select count(*) from refrentail_check) < 100``` |
| Compare the source table and target table output (by default @ is the delimiter. can be overriden by query_dq_delimiter atttribute in rules table)| customer_missing_count_threshold | validity | The_alias_within_the_curly_bracket_is_added_to_the_expectation_which_gets_resolved_at_compile_time_with_alias_values```((select count(*) from ({source_f1}) a join ({source_f2}) b on a.customer_id = b.customer_id) - (select count(*) from ({target_f1}) a join ({target_f2}) b on source_column = target_column)) > ({target_f3})@source_f1@select column, count(*) from source_tbl group by column@source_f2@select column2, count(*) from table2 group by column2@target_f1@select column, count(*) from target_tbl  group by column@target_f2@select column2, count(*) from target_tbl2 group by column2@target_f3@select count(*) from source_tbl ``` |















