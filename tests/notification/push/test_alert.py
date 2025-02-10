from pyspark.sql.types import StructField, IntegerType, StringType, StructType, TimestampType, FloatType

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core import get_spark_session
from spark_expectations.notifications.push.alert import SparkExpectationsAlert
from pyspark.sql.types import StructType, StructField, StringType
from spark_expectations.core.context import SparkExpectationsContext
spark = get_spark_session()

context = SparkExpectationsContext("product_id", spark)
alert = SparkExpectationsAlert(context)





# def test_get_report_data():
#     assert alert.get_report_data("report_type") == ([], [], 0)



def test_prep_report_data():
    default_template = """

<style>
    table {
        border-collapse: collapse;
        width: 60%; /* Reduced width from 100% to 80% */
        font-family: Arial, sans-serif;
        border: 2px solid black; /* Added black border for the table */
    }
    th, td {
        border: 1px solid black; /* Changed to black border */
        text-align: left;
        padding: 4px; /* Reduced padding from 8px to 6px */
    }
    th {
        background-color: #add8e6; /* Changed to light blue */
    }
    tr:nth-child(even) {
        background-color: #f9f9f9;
    }
    .fail {
        background-color: orange; /* Changed to orange */
        color: black;
    }
</style>

{% macro render_table(headers, rows) %}
<table border=1>
    <thead>
        <tr>
            {% for header in headers %}
                <th>{{ header }}</th>
            {% endfor %}
        </tr>
    </thead>
    <tbody>
        {% for row in rows %}
            <tr>
                {% for cell in row %}
                    {% if cell == 'fail' or cell == 'FAIL' %}
                        <td class="fail">{{ cell }}</td>
                    {% else %}
                        <td>{{ cell }}</td>
                    {% endif %}
                {% endfor %}
            </tr>
        {% endfor %}
    </tbody>
</table>
{% endmacro %}

<h2>{{ title }}</h2>
{{ render_table(headers, rows) }}
    """

    # Define schema
    schema = StructType([
        StructField("rule", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("dq_time", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("total_records", StringType(), True),
        StructField("failed_records", IntegerType(), True),
        StructField("valid_records", StringType(), True),
        StructField("success_percentage", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("job", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("Snapshot", StringType(), True),
        StructField("data_object_name", StringType(), True),
        StructField("meta_dq_run_id", StringType(), True),
        StructField("meta_dq_run_datetime", StringType(), True)
    ])

    # Create data
    data = [
        ("product_missing_count_threshold",
         '"product_id":"FUR-BO-10001798","order_id":"CA-2016-152156","order_date":"11/8/2016"', "2025-02-10 22:25:44",
         "your_product", "dq_spark_dev.customer_order", "NULL", '"count":1', 1, '"count":2', 50.0,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("product_missing_count_threshold",
         '"product_id":"FUR-CH-10000454","order_id":"CA-2016-152156","order_date":"11/8/2016"', "2025-02-10 22:25:44",
         "your_product", "dq_spark_dev.customer_order", "NULL", '"count":1', 1, '"count":2', 50.0,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("product_missing_count_threshold",
         '"product_id":"FUR-TA-10000577","order_id":"US-2015-108966","order_date":"10/11/2015"', "2025-02-10 22:25:44",
         "your_product", "dq_spark_dev.customer_order", "NULL", '"count":1', 1, '"count":2', 50.0,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("product_missing_count_threshold",
         '"product_id":"OFF-LA-10000240","order_id":"CA-2016-138688","order_date":"6/12/2016"', "2025-02-10 22:25:44",
         "your_product", "dq_spark_dev.customer_order", "NULL", '"count":1', 1, '"count":2', 50.0,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("product_missing_count_threshold",
         '"product_id":"OFF-ST-10000760","order_id":"US-2015-108966","order_date":"10/11/2015"', "2025-02-10 22:25:44",
         "your_product", "dq_spark_dev.customer_order", "NULL", '"count":1', 1, "NULL", 100.0,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("product_missing_count_threshold", "testing_sample", "2025-02-10 22:25:43", "your_product",
         "dq_spark_dev.customer_order", "fail", "8", 8, "NULL", None,
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15",
         "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("ship_mode_in_set", "ship_mode", "2025-02-10 22:25:43", "your_product", "dq_spark_dev.customer_order", "pass",
         "8", 0, "8", 100.0, "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA",
         "2024-04-15", "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("sales_greater_than_zero", "sales", "2025-02-10 22:25:43", "your_product", "dq_spark_dev.customer_order",
         "pass", "8", 0, "8", 100.0, "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9",
         "na_CORL_DIGITAL_source_to_o9", "NA", "2024-04-15", "NULL",
         "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36"),
        ("discount_threshold", "discount", "2025-02-10 22:25:43", "your_product", "dq_spark_dev.customer_order", "pass",
         "8", 0, "8", 100.0, "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "na_CORL_DIGITAL_source_to_o9", "NA",
         "2024-04-15", "NULL", "your_product_d9927e12-e7cf-11ef-877c-4240eb7a97f9", "2025-02-10 16:55:36")
    ]

    # Create DataFrame

    # Create DataFrame
    df_report_table_test = spark.createDataFrame(data, schema)
    # template_dir = '../../spark_expectations/config/templates'
    # env_loader = Environment(loader=FileSystemLoader(template_dir))
    # template = env_loader.get_template('advanced_email_alert_template.jinja')

    context.set_default_template(default_template)


    context.set_mail_subject("test_mail_subject")
    context.set_to_mail("sudeepta.pal")
    context.set_service_account_password("wp=Wq$37#UI?Ijy7_HNU")
    context.set_mail_smtp_server("smtp.office365.com")
    context.set_mail_smtp_port(587)
    context.set_service_account_email("a.dsm.pss.obs@nike.com")
    context.set_df_dq_obs_report_dataframe(df_report_table_test)

    html_data, mail_subject, mail_receivers_list = alert.prep_report_data()
    assert isinstance(html_data, str)
    assert isinstance(mail_subject, str)
    assert isinstance(mail_receivers_list, str)