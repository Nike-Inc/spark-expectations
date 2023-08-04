import os
import subprocess

# setting up env for local
os.environ["SPARKEXPECTATIONS_ENV"] = "local"

from spark_expectations.core import get_spark_session

spark = get_spark_session()


def main() -> None:
    os.environ["DQ_SPARK_EXPECTATIONS_CERBERUS_TOKEN"] = ""
    current_dir = os.path.dirname(os.path.abspath(__file__))

    print("Creating the necessary infrastructure for the tests to run locally!")

    # run kafka locally in docker
    print("create or run if exist docker container")
    os.system(f"sh {current_dir}/docker_scripts/docker_nsp_start_script.sh")

    # create database
    os.system("rm -rf /tmp/hive/warehouse/dq_spark_local.db")
    spark.sql("create database if not exists dq_spark_local")
    spark.sql("use dq_spark_local")

    # create project_rules_table
    spark.sql("drop table if exists dq_rules")
    os.system("rm -rf /tmp/hive/warehouse/dq_spark_local.db/dq_rules")

    spark.sql(
        """
    create table dq_rules (
    product_id STRING,
    table_name STRING,
    rule_type STRING,
    rule STRING,
    column_name STRING,
    expectation STRING,
    action_if_failed STRING,
    tag STRING,
    description STRING,
    enable_for_source_dq_validation BOOLEAN, 
    enable_for_target_dq_validation BOOLEAN,
    is_active BOOLEAN,
    enable_error_drop_alert BOOLEAN,
    error_drop_threshold INT
    )
    USING delta
    """
    )

    spark.sql(
        "ALTER TABLE dq_rules ADD CONSTRAINT rule_type_action CHECK (rule_type in ('row_dq', 'agg_dq', 'query_dq'));"
    )

    spark.sql(
        "ALTER TABLE dq_rules ADD CONSTRAINT action CHECK ((rule_type = 'row_dq' and action_if_failed IN ('ignore', 'drop', 'fail')) or "
        "(rule_type = 'agg_dq' and action_if_failed in ('ignore', 'fail')) or (rule_type = 'query_dq' and action_if_failed in ('ignore', 'fail')));"
    )

    # create project_dq_stats_table
    # spark.sql("drop table if exists dq_stats")
    # os.system("rm -rf /tmp/hive/warehouse/dq_spark_local.db/dq_stats")
    # spark.sql(
    #     """
    # create table dq_stats (
    # product_id STRING,
    # table_name STRING,
    # input_count LONG,
    # error_count LONG,
    # output_count LONG,
    # output_percentage FLOAT,
    # success_percentage FLOAT,
    # error_percentage FLOAT,
    # source_agg_dq_results array<map<string, string>>,
    # final_agg_dq_results array<map<string, string>>,
    # source_query_dq_results array<map<string, string>>,
    # final_query_dq_results array<map<string, string>>,
    # row_dq_res_summary array<map<string, string>>,
    # dq_status map<string, string>,
    # dq_run_time map<string, float>,
    # dq_rules map<string, map<string,int>>,
    # meta_dq_run_id STRING,
    # meta_dq_run_date DATE,
    # meta_dq_run_datetime TIMESTAMP
    # )
    # USING delta
    # """
    # )

    spark.sql(
        """
    insert into table dq_rules values
    ("your_product", "dq_spark_local.customer_order",  "row_dq", "customer_id_is_not_null", "customer_id", "customer_id is not null","drop", "validity", "customer_id ishould not be null", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "row_dq", "sales_greater_than_zero", "sales", "sales > 0", "drop", "accuracy", "sales value should be greater than zero", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "row_dq", "discount_threshold", "discount", "discount*100 < 60","drop", "validity", "discount should be less than 40", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "row_dq", "ship_mode_in_set", "ship_mode", "lower(trim(ship_mode)) in('second class', 'standard class', 'standard class')", "drop", "validity", "ship_mode mode belongs in the sets", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "row_dq", "profit_threshold", "profit", "profit>0", "drop", "validity", "profit threshold should be greater tahn 0", true, true, true, true, 0)
    
    ,("your_product", "dq_spark_local.customer_order", "agg_dq", "sum_of_sales", "sales", "sum(sales)>10000", "ignore", "validity", "regex format validation for quantity",  true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "agg_dq", "sum_of_quantity", "quantity", "sum(sales)>10000", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "agg_dq", "distinct_of_ship_mode", "ship_mode", "count(distinct ship_mode)<=3", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "agg_dq", "row_count", "*", "count(*)>=10000", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0)

    ,("your_product", "dq_spark_local.customer_order", "query_dq", "product_missing_count_threshold", "*", "((select count(distinct product_id) from product) - (select count(distinct product_id) from order))>(select count(distinct product_id) from product)*0.2", "ignore", "validity", "row count threshold", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "query_dq", "product_category", "*", "(select count(distinct category) from product) < 5", "ignore", "validity", "distinct product category", true, true, true, false, 0)
    ,("your_product", "dq_spark_local.customer_order", "query_dq", "row_count_in_order", "*", "(select count(*) from order)<10000", "ignore", "accuracy", "count of the row in order dataset", true, true, true, false, 0)
     """
    )

    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "referential_integrity_customer_id", "customer_id",
    #    "customer_id in(select distinct customer_id from customer)", true, true, "drop", true, "validity",
    #    "referential integrity for cuatomer_id")
    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "referential_integrity_product_id", "product_id",
    #    "select count(*) from (select distinct product_id as ref_product from product) where product_id=ref_product > 1",
    #    true, true, "drop", true, "validity", "referntial integrity for product_id")
    # , (
    # "your_product", "dq_spark_local.customer_order", "row_dq", "regex_format_sales", "sales", "sales rlike '[1-9]+.[1-9]+'",
    # true, true, "drop", true, "validity", "regex format validation for sales")
    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "regex_format_quantity", "quantity",
    #    "quantity rlike '[1-9]+.[1-9]+'", true, true, "drop", true, "validity", "regex format validation for quantity")
    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "date_format_order_date", "order_date",
    #    "order_date rlike '([1-3][1-9]|[0-1])/([1-2]|[1-9])/20[0-2][0-9]''", true, true, "drop", true, "validity",
    #    "regex format validation for quantity")
    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "regex_format_order_id", "order_id",
    #    "order_id rlike '(US|CA)-20[0-2][0-9]-*''", true, true, "drop", true, "validity",
    #    "regex format validation for quantity")

    # , ("your_product", "dq_spark_local.employee_new", "query_dq", "", "*",
    #    "(select count(*) from dq_spark_local_employee_new)!=(select count(*) from dq_spark_local_employee_new)", true,
    #    false, "ignore", false, "validity", "canary check to comapre the two table count")
    # , ("your_product", "dq_spark_local.employee_new", "query_dq", "department_salary_threshold", "department",
    #    "(select count(*) from (select department from dq_spark_local_employee_new group by department having sum(bonus)>1000))<1",
    #    true, false, "ignore", true, "validity", "each sub-department threshold")
    # , (
    # "your_product", "dq_spark_local.employee_new", "query_dq", "count_of_exit_date_nulls_threshold", "exit_date", "", true,
    # true, "ignore", false, "validity", "exit_date null threshold")

    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "complete_duplicate", "*",
    #    "count(*) over(partition by customer_id,product_id,order_id,order_date,ship_date,ship_mode,sales,quantity,discount,profit order by 1)",
    #    true, true, "drop", true, "validity", "complete duplicate record")
    # , ("your_product", "dq_spark_local.customer_order", "row_dq", "primary_key_check", "*",
    #    "count(*) over(partition by customer_id, order_id order by 1)", true, true, "drop", true, "validity",
    #    "primary key check")

    # ,("your_product", "dq_spark_local.customer_order", "row_dq", "order_date_format_check", "order_date", "to_date(order_date, 'dd/MM/yyyy')", true, true,"drop" ,true, "validity", "Age of the employee should be less than 65")

    spark.sql("select * from dq_rules").show(truncate=False)

    # DROP the data tables and error tables
    spark.sql("drop table if exists dq_spark_local.customer_order")
    os.system(
        "rm -rf /tmp/hive/warehouse/dq_spark_local.db/dq_spark_local.customer_order"
    )

    spark.sql("drop table if exists dq_spark_local.customer_order_error")
    os.system(
        "rm -rf /tmp/hive/warehouse/dq_spark_local.db/dq_spark_local.customer_order_error"
    )

    print("Local infrastructure setup is done")


if __name__ == "__main__":
    main()
