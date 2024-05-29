import os
from pyspark.sql.session import SparkSession

os.environ["SPARKEXPECTATIONS_ENV"] = "local"

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

RULES_TABLE_SCHEMA = """ ( product_id STRING,
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
    error_drop_threshold INT ,
    query_dq_delimiter STRING,
    enable_querydq_custom_output BOOLEAN
    )
"""


RULES_DATA = """

     ("your_product", "dq_spark_dev.customer_order", "row_dq", "sales_greater_than_zero", "sales", "sales > 2", "ignore", "accuracy", "sales value should be greater than zero", false, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "discount_threshold", "discount", "discount*100 < 60","drop", "validity", "discount should be less than 40", true, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "ship_mode_in_set", "ship_mode", "lower(trim(ship_mode)) in('second class', 'standard class', 'standard class')", "drop", "validity", "ship_mode mode belongs in the sets", true, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_{env}.customer_order", "row_dq", "profit_threshold", "profit", "profit>0", "ignore", "validity", "profit threshold should be greater tahn 0", false, true, false, true, 0,null, null)
    ,("your_product", "dq_spark_dev.customer_order", "agg_dq", "sum_of_sales_range type 1", "sales", "sum(sales)>99 and sum(sales)<99999", "ignore", "validity", "regex format validation for quantity",  true, true, true, false, 0, null, true)
    ,("your_product", "dq_spark_dev.customer_order", "agg_dq", "sum_of_sales_range type 2", "sales", "sum(sales) between 100 and 10000 ", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0, null, true)
    ,("your_product", "dq_spark_dev.customer_order", "agg_dq", "sum_of_sales", "sales", "sum(sales)>10000", "ignore", "validity", "regex format validation for quantity",  true, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_dev.customer_order", "agg_dq", "sum_of_quantity", "quantity", "sum(quantity)>10000", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0,null, null)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "product_missing_count_threshold", "*", "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@select distinct product_id,order_id from order_target", "ignore", "validity", "row count threshold", true, true, true, false, 0,null, true)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "customer_missing_count_threshold","*", "((select count(*) from ({source_f1}) a join ({source_f2}) b on a.customer_id = b.customer_id) - (select count(*) from ({target_f1}) a join ({target_f2}) b on a.customer_id = b.customer_id)) > ({target_f3})@source_f1@select customer_id, count(*) from customer_source group by customer_id@source_f2@select customer_id, count(*) from order_source group by customer_id@target_f1@select customer_id, count(*) from customer_target group by customer_id@target_f2@select customer_id, count(*) from order_target group by customer_id@target_f3@select count(*) from order_source", "ignore", "validity", "customer count threshold", true, true, true, false, 0,null, true)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "order_count_validity", "*", "({source_f1}) > 10@source_f1@select count(*) from order_source", "ignore", "validity", "row count threshold", true, true, true, false, 0, "@", true)
    ,("your_product", "dq_spark_dev.customer_order", "query_dq", "order_count_validity_check", "*", "(select count(*) from order_source) > 10", "ignore", "validity", "row count threshold", true, true, true, false, 0, null, true)
    ,("your_product", "dq_spark_{env}.customer_order", "query_dq", "product_category", "*", "(select count(distinct category) from {table}) < 5", "ignore", "validity", "distinct product category", true, true, true, false, 0,null, true)
    ,("your_product", "dq_spark_{env}.customer_order", "agg_dq", "distinct_of_ship_mode", "ship_mode", "count(distinct ship_mode) <= 3", "ignore", "validity", "regex format validation for quantity", true, true, true, false, 0,null, null)


    """


def set_up_kafka() -> None:
    print("create or run if exist docker container")
    os.system(f"sh {CURRENT_DIR}/docker_scripts/docker_kafka_start_script.sh")


def add_kafka_jars(builder: SparkSession.builder) -> SparkSession.builder:
    return builder.config(  # below jars are used only in the local env, not coupled with databricks or EMR
        "spark.jars",
        f"{CURRENT_DIR}/../../jars/spark-sql-kafka-0-10_2.12-3.0.0.jar,"
        f"{CURRENT_DIR}/../../jars/kafka-clients-3.0.0.jar,"
        f"{CURRENT_DIR}/../../jars/commons-pool2-2.8.0.jar,"
        f"{CURRENT_DIR}/../../jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar",
    )


def set_up_iceberg() -> SparkSession:
    set_up_kafka()
    spark = add_kafka_jars(
        SparkSession.builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/hive/warehouse")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "/tmp/hive/warehouse")
    ).getOrCreate()

    os.system("rm -rf /tmp/hive/warehouse/dq_spark_local")

    spark.sql("create database if not exists spark_catalog.dq_spark_local")
    spark.sql(" use spark_catalog.dq_spark_local")

    spark.sql("drop table if exists dq_spark_local.dq_stats")

    spark.sql("drop table if exists dq_spark_local.dq_rules")

    spark.sql(
        f" CREATE TABLE dq_spark_local.dq_rules {RULES_TABLE_SCHEMA} USING ICEBERG"
    )
    spark.sql(f" INSERT INTO dq_spark_local.dq_rules  values {RULES_DATA} ")

    spark.sql("select * from dq_spark_local.dq_rules").show(truncate=False)
    return spark


def set_up_bigquery(materialization_dataset: str) -> SparkSession:
    set_up_kafka()
    spark = add_kafka_jars(
        SparkSession.builder.config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0",
        )
    ).getOrCreate()
    spark._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", materialization_dataset)

    # Add dependencies like gcs-connector-hadoop3-2.2.6-SNAPSHOT-shaded.jar, spark-avro_2.12-3.4.1.jar if you wanted to use indirect method for reading/writing
    return spark


def set_up_delta() -> SparkSession:
    set_up_kafka()

    builder = add_kafka_jars(
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "/tmp/hive/warehouse")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .config("spark.jars.ivy", "/tmp/ivy2")
    )
    spark = builder.getOrCreate()

    os.system("rm -rf /tmp/hive/warehouse/dq_spark_dev.db")

    spark.sql("create database if not exists dq_spark_dev")
    spark.sql("use dq_spark_dev")

    spark.sql("drop table if exists dq_stats")

    spark.sql("drop table if exists dq_rules")

    spark.sql(f" CREATE TABLE dq_rules {RULES_TABLE_SCHEMA} USING DELTA")

    spark.sql(f" INSERT INTO dq_rules  values {RULES_DATA}")

    spark.sql("select * from dq_rules").show(truncate=False)

    return spark
