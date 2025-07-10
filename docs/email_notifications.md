# Spark Expectations Email Notifications/Alerts

Spark Expectations can send three kinds of emails.

## 1. Data Quality Report Emails

Please see the [Observability Examples doc](Observability_examples) for more detailed information on emails that contain the DQ report.


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

The following two attributes in the user configuration should be set to enable custom metrics emails:
```python
user_config.se_notifications_enable_custom_email_body: True,
user_config.se_notifications_email_custom_body: "custom stats: 'product_id': {}"
```
The `se_notifications_email_custom_body` field needs to comply with a specific syntax.

### Example Custom Metrics Config

The following is a more comprehensive configuration for the custom email body, showing how to request more metrics.
```python
user_config.se_notifications_email_custom_body: (
        "Custom statistics:\n "
        "'product_id': {},\n "
        "'table_name': {},\n "
        "'input_count': {},\n "
        "'error_count': {},\n "
        "'output_count': {},\n "
        "'output_percentage': {},\n "
        "'success_percentage': {},\n "
        "'error_percentage': {},\n "
        "'dq_status': {},\n "
        "'dq_run_time': {},\n "
        "'dq_rules': {},\n "
        "'meta_dq_run_id': {},\n "
        "'meta_dq_run_date': {},\n "
        "'meta_dq_run_datetime': {},\n "
        "'dq_env': {}\n"
    )
```