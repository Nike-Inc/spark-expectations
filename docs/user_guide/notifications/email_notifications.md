By default email notifications are disabled. To use them we need to pass required user configuration for spark-expectation to properly run.


### Notification Config Parameters


!!! info "user_config.se_notifications_enable_email"
    Master toggle to Enable Email Notifications


??? info "Notification triggers"
    These parameters control **when** email notifications are sent during Spark-Expectations runs.  
    `Hover over each parameter to see a short description.`
       
    - <abbr title="Master toggle to enable email notifications">user_config.se_notifications_enable_email</abbr>
    - <abbr title="Enable notifications when job starts">user_config.se_notifications_on_start</abbr>
    - <abbr title="Enable notifications when job ends">user_config.se_notifications_on_completion</abbr>
    - <abbr title="Enable notifications on failure">user_config.se_notifications_on_fail</abbr>
    - <abbr title="Notify if error drop threshold is breached">user_config.se_notifications_on_error_drop_exceeds_threshold_breach</abbr>
    - <abbr title="Notify if rules with action 'ignore' fail">user_config.se_notifications_on_rules_action_if_failed_set_ignore</abbr>
    - <abbr title="Threshold value for error drop notifications">user_config.se_notifications_on_error_drop_threshold</abbr>


??? info "SMTP Config"
    These parameters control **what** email server to use for sending the notifications.  
    `Hover over each parameter to see a short description.`

    - <abbr title="SMTP host for sending emails">user_config.se_notifications_email_smtp_host</abbr>
    - <abbr title="SMTP port for sending emails">user_config.se_notifications_email_smtp_port</abbr>


??? info "Email Content"
    These parameters control **how** email content is going to look like.
    `Hover over each parameter to see a short description.` 

    - <abbr title="Sender email address">user_config.se_notifications_email_from</abbr>
    - <abbr title="Receiver email address">user_config.se_notifications_email_to_other_mail_id</abbr>
    - <abbr title="Email subject">user_config.se_notifications_email_subject</abbr>
    - <abbr title="Custom email body content">user_config.se_notifications_email_custom_body</abbr>


??? info "Email Templates"
    These parameters **configure** usage of jinja2 templates 
    
    - <abbr title="Enable usage of templates for body of basic email notifications">user_config.se_notifications_enable_templated_basic_email_body</abbr>
    - <abbr title="If provided it will use custom jinja template in place of a default one">user_config.se_notifications_default_basic_email_template</abbr>



### Configure SMTP Notifications

To enable email notifications (such as alerts for data quality failures) in Spark-Expectations, you need to configure SMTP settings. 
You can reference the `user_config.py` file in the `spark_expectations/config` directory to access / setup the SMTP configuration parameters. This file should contain the necessary SMTP/email notification settings for Spark-Expectations.

#### Verifying SMTP Parameters
Before using SMTP notifications, verify that the following parameters are set correctly in your configuration (see `user_config.py` for the exact constant names):

- SMTP server host
- SMTP server port
- SMTP username
- SMTP password
- Sender email address (`from`)
- Recipient email address(es) (`to`)
- Enable/disable SMTP authentication and TLS as needed

??? info "Sample Python Script to Send a Test Email"

    You can use the following Python script to test your SMTP configuration. This script will send a test email using the configured SMTP settings. Make sure to replace the placeholders with your actual SMTP configuration values.
    ```python
    from email.mime.text import MIMEText

    smtp_host = "smtp.example.com"
    smtp_port = 587
    smtp_user = "your_email@example.com"
    smtp_password = "your_password"
    smtp_from = "your_email@example.com"
    smtp_to = "recipient@example.com"

    msg = MIMEText("This is a test email from Spark-Expectations SMTP setup.")
    msg["Subject"] = "Spark-Expectations SMTP Test"
    msg["From"] = smtp_from
    msg["To"] = smtp_to

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_from, [smtp_to], msg.as_string())

    print("Test email sent successfully.")
    ```
**Note:**
Never commit sensitive credentials (like SMTP passwords) to version control. Use environment variables or a secure secrets manager.
Make sure your SMTP server allows connections from your environment (some providers may require app passwords or special settings).

## User Configuration Example

???+ note "Show example user configuration"
    ```python
    user_conf_dict = {
        # Master Toggle
        user_config.se_notifications_enable_email: True,
        
        # Notification triggers
        user_config.se_notifications_on_start: True,
        user_config.se_notifications_on_completion: True,
        user_config.se_notifications_on_fail: True,
        user_config.se_notifications_on_error_drop_exceeds_threshold_breach: True,
        user_config.se_notifications_on_rules_action_if_failed_set_ignore: True,
        user_config.se_notifications_on_error_drop_threshold: 15,
        
        # Email notification server config
        user_config.se_notifications_email_smtp_host: "mailpit",
        user_config.se_notifications_email_smtp_port: 1025,
        
        # Email headers (sender, receiver, subject)
        user_config.se_notifications_email_from: "sender@example.com",
        user_config.se_notifications_email_to_other_mail_id: "receiver@example.com",
        user_config.se_notifications_email_subject: "Test Subject",
        
        # Email content
        user_config.se_notifications_email_custom_body: "This is a custom email body from test.",

        # Email Templates
        user_config.se_notifications_enable_templated_basic_email_body: True,

        # If jinjia template is not provided it will use default one
        user_config.se_notifications_default_basic_email_template: custom_html_email_template
    }
    ```
### Template Example

??? note "Show custom template example"
    ```python
    custom_html_email_template = """
    <style>
        table {
            border-collapse: collapse;
            width: 60%;
            font-family: Arial, sans-serif;
            border: 2px solid black;
        }
        td {
            border: 1px solid black;
            text-align: left;
            padding: 4px;
            background-color: #00ff1a;
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

    ```
