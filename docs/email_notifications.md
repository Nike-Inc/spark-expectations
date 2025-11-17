# Spark Expectations Email Notifications/Alerts

Spark Expectations can send three kinds of emails.

## 1. Data Quality Report Emails

Please see the [Observability Examples doc](Observability_examples.md) for more detailed information on emails that contain the DQ report.


## 2. Basic Email Notifications/Alerts

In addition to the email alerts described above for the report table, Spark Expectations can also send basic email alerts on a job's start, completion, failure, and/or other conditions depending on user configuration. These alerts have a structure like:

```
Spark expectations job has started
table_name: <Name of the Table>
run_id: <Run ID>
run_date: <Run Date>
```

Basic notifications only report the table name, run ID, and run date and this is not configurable at this time. For emails where you can choose specific metrics see the [custom metrics email section](#3-custom-metrics-emails) below.

The configurations related to basic email notifications are:

```python
    user_config.se_notifications_enable_email: True,
    user_config.se_notifications_on_start: True,
    user_config.se_notifications_on_completion: True,
    user_config.se_notifications_on_fail: True,
    user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
    user_config.se_notifications_on_rules_action_if_failed_set_ignore: True,
    user_config.se_notifications_on_error_drop_threshold: 15,
    # smtp details for sending emails
    user_config.se_notifications_email_smtp_host: "smtp.######.com",
    user_config.se_notifications_email_smtp_port: 587,
    user_config.se_notifications_smtp_password: "************",
    user_config.se_notifications_email_from: "sender@mail.com",
    user_config.se_notifications_email_to_other_mail_id: "receiver@mail.com"
    user_config.se_notifications_email_subject: "Spark Expectations - Notification"
```

### HTML Template Options for Basic Emails

Spark Expectations supports the use of Jinja templates to apply HTML to the basic email notifications. 

*Please note that the template for basic email notifications is different and separate from the template for the DQ report emails.*

To enable Jinja templates for basic email alerts set these two optional parameters:
```python
    user_config.se_notifications_enable_templated_basic_email_body: True
    user_config.se_notifications_default_basic_email_template: ""
```

If a template is not provided the default template located in `spark_expectations/config/templates/basic_email_alert_template.jinja` will be used.

This feature is somewhat limited currently and the template should be set up to handle `rows`.


### Example Template Config Setup
```python
basic_html_template = """
<style>
    table {
        border-collapse: collapse;
        width: 60%; /* Reduced width from 100% to 80% */
        font-family: Arial, sans-serif;
        border: 2px solid black; /* Added black border for the table */
    }
    td {
        border: 1px solid black; /* Changed to black border */
        text-align: left;
        padding: 4px;
    }
    tr:nth-child(even) {
        background-color: #f9f9f9;
    }
</style>

{% macro render_table(rows) %}
<table border=1>
    <tbody>
        {% for row in rows %}
            <tr>
                {% for cell in row %}
                        <td>{{ cell }}</td>
                {% endfor %}
            </tr>
        {% endfor %}
    </tbody>
</table>
{% endmacro %}

<h3>{{ title }}</h3>
{{ render_table(rows) }}
"""

user_config.se_notifications_enable_templated_basic_email_body: True
user_config.se_notifications_default_basic_email_template: basic_html_template
```

## 3. Custom Metrics Emails

The following two attributes in the user configuration should be set to enable custom metrics emails (plain text by default):
```python
user_config.se_notifications_enable_custom_email_body: True,
user_config.se_notifications_email_custom_body: "custom stats: 'product_id': {}"
```
The `se_notifications_email_custom_body` field needs to comply with a specific syntax, matching the style in the example below. The metrics that can be requested are the names of the columns in the __dq_stats__ table.

### Example Custom Metrics Config

The following is a comprehensive configuration for the custom email body, showing how to request more metrics.
```python
user_config.se_notifications_email_custom_body: (
        "'product_id': {},\n "
        "'table_name': {},\n "
        "'input_count': {},\n "
        "'error_count': {},\n "
        "'output_count': {},\n "
        "'output_percentage': {},\n "
        "'success_percentage': {},\n "
        "'error_percentage': {},\n "
        "'source_agg_dq_results': {},\n "
        "'source_query_dq_results': {},\n "
        "'final_agg_dq_results': {},\n "
        "'final_query_dq_results': {},\n "
        "'row_dq_res_summary': {},\n "
        "'row_dq_error_threshold': {},\n "
        "'dq_status': {},\n "
        "'dq_run_time': {},\n "
        "'dq_rules': {},\n "
        "'meta_dq_run_id': {},\n "
        "'meta_dq_run_date': {},\n "
        "'meta_dq_run_datetime': {},\n "
        "'dq_env': {}\n"
    )
```
By default the custom emails are in plain text, but there is an option to format them using an HTML template.

### Using Custom Jinja2 HTML Templates

Set `user_config.se_notifications_enable_templated_custom_email` and `user_config.se_notifications_email_custom_template` to enable using a Jinja2 HTML template to format the custom metrics email. The example below includes an example template definition in addition to the config.

```python
custom_html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>DQ Results Summary</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            color: #333;
            margin: 20px;
        }
        dl {
            border: 1px solid #ccc;
            padding: 12px;
            width: 400px;
            background-color: #f9f9f9;
        }
        dt {
            font-weight: bold;
            margin-top: 8px;
        }
        dd {
            margin: 0 0 8px 16px;
        }
    </style>
</head>
<body>

<dl>
    <dt>Product</dt>
    <dd>{{ product_id }}</dd>

    <dt>Table</dt>
    <dd>{{ table_name }}</dd>

    <dt>Source AGG DQ Check Name [0]</dt>
    <dd>{{ source_agg_dq_results[0].rule }}</dd>

    <dt>Source AGG DQ Check Description [0]</dt>
    <dd>{{ source_agg_dq_results[0].description }}</dd>

    <dt>Source AGG DQ Check Result [0]</dt>
    <dd>{{ source_agg_dq_results[0].status }}</dd>

    <dt>Source AGG DQ Check Name [1]</dt>
    <dd>{{ source_agg_dq_results[1].rule }}</dd>

    <dt>Source AGG DQ Check Description [1]</dt>
    <dd>{{ source_agg_dq_results[1].description }}</dd>

    <dt>Source AGG DQ Check Result [1]</dt>
    <dd>{{ source_agg_dq_results[1].status }}</dd>

    <dt>Source Query DQ Check Name</dt>
    {% for item in source_query_dq_results %}
        <dd>{{ item.rule }}</dd>
    {% endfor %}

    <dt>Source Query DQ Check Description</dt>
    {% for item in source_query_dq_results %}
        <dd>{{ item.description }}</dd>
    {% endfor %}

    <dt>Source Query DQ Check Result</dt>
    {% for item in source_query_dq_results %}
        <dd>{{ item.status }}</dd>
    {% endfor %}

    <dt>Rule Execution Timestamp</dt>
    <dd>{{ meta_dq_run_datetime }}</dd>

    <dt>Final AGG DQ Results</dt>
    <dd>{{ final_agg_dq_results }}</dd>

</dl>

</body>
</html>
"""

user_config.se_notifications_enable_templated_custom_email: True
user_config.se_notifications_email_custom_template: ""
```
As seen in the template, some of the results values are nested data structures (often an array of maps/dicts) and their elements can be accessed directly with the right syntax, or using `for` loops or other options that Jinja2 supports.

__Note:__
- If an empty string `""` is passed in to the template field in the user config, the default template will be used: ([spark_expectations/config/templates/custom_email_alert_template.jinja](../spark_expectations/config/templates/custom_email_alert_template.jinja)).
- A metric name must be specified in `user_config.se_notifications_email_custom_body` if it is being referenced in the template. For example, if `{{ final_agg_dq_results }}` is referenced in the template, then `"'final_agg_dq_results': {},\n "` must be in the user config.