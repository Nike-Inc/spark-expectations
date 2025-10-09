spark-expectations relies on the [PagerDuty Events API V2](https://developer.pagerduty.com/docs/events-api-v2-overview) to create new incidents. An existing PagerDuty [service needs to be created](https://support.pagerduty.com/main/docs/services-and-integrations#create-a-service) before incidents can be created from spark-expectations.

### Pre-requisites

By default PagerDuty notifications (or the ability to create incidents) are disabled. To use them we need to pass the required user configurations for spark-expectations to properly run. 

### Notification Config Parameters

!!! info "user_config.se_notifications_enable_pagerduty"
    Master toggle to enable PagerDuty notifications (this will create incidents for your service!)


??? info "Notification triggers"
    These parameters control **when** notifications are sent during Spark-Expectations runs. This would create a new incident per enabled trigger.  
    `Hover over each parameter to see a short description.`
       
    - <abbr title="Master toggle to enable PagerDuty notifications">user_config.se_notifications_enable_pagerduty</abbr>
    - <abbr title="Enable notifications when job starts">user_config.se_notifications_on_start</abbr>
    - <abbr title="Enable notifications when job ends">user_config.se_notifications_on_completion</abbr>
    - <abbr title="Enable notifications on failure">user_config.se_notifications_on_fail</abbr>
    - <abbr title="Notify if error drop threshold is breached">user_config.se_notifications_on_error_drop_exceeds_threshold_breach</abbr>
    - <abbr title="Notify if rules with action 'ignore' fail">user_config.se_notifications_on_rules_action_if_failed_set_ignore</abbr>
    - <abbr title="Threshold value for error drop notifications">user_config.se_notifications_on_error_drop_threshold</abbr>


??? info "PagerDuty Configs"
    Additional configurations that are needed to be able to create incidents with spark-expectations.
    `Hover over each parameter to see a short description.`

    - <abbr title="Integration key that is generated from the PagerDuty Service">user_config.se_notifications_pagerduty_integration_key</abbr>
    - <abbr title="Webhook url for PagerDuty, this should be sent to the events. This is usually set to the events api url ">user_config.se_notifications_pagerduty_webhook_url</abbr>


### User Configuration Example

??? note "Show example user configuration"
    ```python
    user_conf_dict = {
        # Master Toggle
        user_config.se_notifications_enable_pagerduty: True,

        # PagerDuty Configuration
        user_config.se_notifications_pagerduty_integration_key: <enter_integration_key_here>,
        user_config.se_notifications_pagerduty_webhook_url: "https://events.pagerduty.com/v2/enqueue",
    }
    ```

### Links to example notebooks
An example notebook is available to use that sets up PD in a notebook [here](https://github.com/Nike-Inc/spark-expectations/blob/main/notebooks/spark_expectations_basic_pagerduty_notification.ipynb).

This notebook will:

- Grab integration key using databricks secret manager (default)
    - An option to use Cerberus Secrets Manager is present but commented out. Uncomment if you would to use this method instead.
- Configure spark-expectations
- Load sample data and then run some validations rules afterwards.

If everything has been configured correctly, this will create a new incident based on the triggers you have enabled. 

