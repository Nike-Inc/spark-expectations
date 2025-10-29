from unittest.mock import patch, Mock
import pytest
import requests
from spark_expectations.core.exceptions import SparkExpectationsPagerDutyException
from spark_expectations.notifications.plugins.pagerduty import SparkExpectationsPagerDutyPluginImpl


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True)
def test_send_incident_success(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/change/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"
    _mock_context.product_id = "test_product"
    _mock_context.get_table_name = "test_table"

    _config_args = {"message": "Spark expectations job has been failed"}

    # Mock requests.post to return a response with status code 200
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 202
        mock_post.return_value = mock_response

        print("TEST")
        print(_mock_context.get_pagerduty_webhook_url)
        print(_mock_context.get_pagerduty_integration_key)
        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        mock_post.assert_called_once_with(
            _mock_context.get_pagerduty_webhook_url,
            json={
                "routing_key": _mock_context.get_pagerduty_integration_key,
                "dedup_key": "spark_expectations_test_product_test_table_failure",
                "event_action": "trigger",
                "payload": {
                    "summary": "Spark expectations job has been failed",
                    "source": "Spark Expectations",
                    "severity": "error",
                },
            },
            headers={"Content-Type": "application/json"},
            timeout=10,
        )


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_exception(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "http://test_webhook_url"
    _config_args = {"message": "Spark expectations job has been failed"}

    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        with pytest.raises(SparkExpectationsPagerDutyException):
            pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
def test_get_cerberus_integration_key(_mock_secret_handler, _mock_get_secret):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "http://cerberus.url",
        "se.streaming.cerberus.sdb.path": "/path/to/secret",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }
    _mock_get_secret.return_value = {"integration_key": "test_integration_key"}
    _mock_secret_handler.get_secret = _mock_get_secret

    integration_key = pagerduty_handler._get_cerberus_integration_key(_mock_secret_handler, pd_secret_dict)
    assert integration_key == "test_integration_key"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
def test_get_cerberus_integration_key_none(_mock_secret_handler, _mock_get_secret):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "http://cerberus.url",
        "se.streaming.cerberus.sdb.path": "/path/to/secret",
        "spark.expectations.notifications.pagerduty.integration.key": None,
    }
    _mock_get_secret.return_value = None
    _mock_secret_handler.get_secret = _mock_get_secret

    integration_key = pagerduty_handler._get_cerberus_integration_key(_mock_secret_handler, pd_secret_dict)
    assert integration_key is None


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
def test_get_databricks_integration_key(_mock_secret_handler, _mock_get_secret):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }
    _mock_get_secret.return_value = "test_integration_key"
    _mock_secret_handler.get_secret = _mock_get_secret

    integration_key = pagerduty_handler._get_databricks_integration_key(_mock_secret_handler, pd_secret_dict)
    assert integration_key == "test_integration_key"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend.get_secret", autospec=True, spec_set=True)
def test_get_databricks_integration_key_none(_mock_secret_handler, _mock_get_secret):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": None,
    }
    _mock_get_secret.return_value = None
    _mock_secret_handler.get_secret = _mock_get_secret

    integration_key = pagerduty_handler._get_databricks_integration_key(_mock_secret_handler, pd_secret_dict)
    assert integration_key is None

@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_integration_key_cerberus(_mock_secret_handler):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "http://cerberus.url",
        "se.streaming.cerberus.sdb.path": "/path/to/secret",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._get_cerberus_integration_key", return_value="test_integration_key") as mock_cerberus_integration_key:
        integration_key = pagerduty_handler._retrieve_integration_key(_mock_secret_handler, "cerberus", pd_secret_dict)
        mock_cerberus_integration_key.assert_called_once_with(_mock_secret_handler, pd_secret_dict)
        assert integration_key == "test_integration_key"

@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_integration_key_databricks(_mock_secret_handler):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._get_databricks_integration_key", return_value="test_integration_key") as mock_databricks_integration_key:
        integration_key = pagerduty_handler._retrieve_integration_key(_mock_secret_handler, "databricks", pd_secret_dict)
        mock_databricks_integration_key.assert_called_once_with(_mock_secret_handler, pd_secret_dict)
        assert integration_key == "test_integration_key"


@patch("spark_expectations.secrets.SparkExpectationsSecretsBackend", autospec=True, spec_set=True)
def test_retrieve_integration_key_none(_mock_secret_handler):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    pd_secret_dict = {
        "se.streaming.secret.type": "not_a_valid_type",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    integration_key = pagerduty_handler._retrieve_integration_key(_mock_secret_handler, "not_a_valid_type", pd_secret_dict)
    assert integration_key is None


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_pd_integration_key_method_with_databricks(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_pagerduty_integration_key = None
    _mock_context.get_pagerduty_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._retrieve_integration_key", return_value="test_integration_key") as mock_retrieve_integration_key:
        pagerduty_handler._get_pd_integration_key(_mock_context)
        mock_retrieve_integration_key.assert_called_once()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_pd_integration_key_method_with_cerberus(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_pagerduty_integration_key = None
    _mock_context.get_pagerduty_creds_dict = {
        "se.streaming.secret.type": "cerberus",
        "se.streaming.cerberus.url": "http://cerberus.url",
        "se.streaming.cerberus.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._retrieve_integration_key", return_value="test_integration_key") as mock_retrieve_integration_key:
        pagerduty_handler._get_pd_integration_key(_mock_context)
        mock_retrieve_integration_key.assert_called_once()

@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_pd_integration_key_method_missing_key_with_key_error(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_pagerduty_integration_key = None
    _mock_context.get_pagerduty_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": None,
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._retrieve_integration_key") as mock_retrieve_integration_key:
        mock_retrieve_integration_key.side_effect = KeyError("KeyError: PagerDuty integration key is missing.")
        with pytest.raises(SparkExpectationsPagerDutyException, match="KeyError: PagerDuty integration key is missing."):
            pagerduty_handler._get_pd_integration_key(_mock_context)


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_pd_integration_key_method_failed_retrieval(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_pagerduty_integration_key = None
    _mock_context.get_pagerduty_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": "integration_key",
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._retrieve_integration_key", side_effect=Exception("Generic error")):
        with pytest.raises(SparkExpectationsPagerDutyException, match="Failed to retrieve PagerDuty integration key."):
            pagerduty_handler._get_pd_integration_key(_mock_context)

@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_get_pd_integration_key_method_missing_key(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_pagerduty_integration_key = None
    _mock_context.get_pagerduty_creds_dict = {
        "se.streaming.secret.type": "databricks",
        "se.streaming.databricks.url": "http://databricks.url",
        "se.streaming.databricks.secret.scope": "my_secret_scope",
        "spark.expectations.notifications.pagerduty.integration.key": None,
    }

    with patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsPagerDutyPluginImpl._retrieve_integration_key") as mock_retrieve_integration_key:
        mock_retrieve_integration_key.return_value = None
        with pytest.raises(SparkExpectationsPagerDutyException, match="PagerDuty integration key is not set."):
            pagerduty_handler._get_pd_integration_key(_mock_context)

@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_pagerduty_disabled(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = False
    _mock_context.get_pagerduty_webhook_url = "http://test_webhook_url"
    _config_args = {"message": "test message"}

    with patch.object(requests, "post") as mock_post:
        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        mock_post.post.assert_not_called()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_skipped_for_non_failure_message(_mock_context):
    """Test that PagerDuty notifications are skipped for non-failure messages like job start/completion"""
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"

    # Test with job start message
    _config_args = {"message": "Spark expectations job has started \n\ntable_name: test_table\nrun_id: 123"}

    with patch.object(requests, "post") as mock_post:
        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        # Should not call post since this is not a failure
        mock_post.assert_not_called()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_skipped_for_completion_message(_mock_context):
    """Test that PagerDuty notifications are skipped for job completion messages"""
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"

    # Test with job completion message
    _config_args = {"message": "Spark expectations job has been completed \n\nproduct_id: test_product"}

    with patch.object(requests, "post") as mock_post:
        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        # Should not call post since this is not a failure
        mock_post.assert_not_called()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True)
def test_send_incident_for_job_failure_message(_mock_context):
    """Test that PagerDuty notifications are sent for job failure messages"""
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"
    _mock_context.product_id = "test_product"
    _mock_context.get_table_name = "test_table"

    # Test with job failure message
    _config_args = {"message": "Spark expectations job has been failed \n\nproduct_id: test_product"}

    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 202
        mock_post.return_value = mock_response

        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        # Should call post since this is a failure
        mock_post.assert_called_once()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True)
def test_send_incident_for_error_threshold_breach(_mock_context):
    """Test that PagerDuty notifications are sent for error threshold breach messages"""
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"
    _mock_context.product_id = "test_product"
    _mock_context.get_table_name = "test_table"

    # Test with error threshold breach message
    _config_args = {"message": "Spark expectations - dropped error percentage has been exceeded above the threshold value(5%) for `row_data` quality validation"}

    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 202
        mock_post.return_value = mock_response

        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        # Should call post since this is an error condition
        mock_post.assert_called_once()


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_skipped_for_ignored_rules_failure(_mock_context):
    """Test that PagerDuty notifications are NOT sent for ignored rules failure messages.
    
    These are informational notifications only and should not create PagerDuty incidents.
    PagerDuty incidents should only be created for actual critical failures.
    """
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "https://events.pagerduty.com/v2/enqueue"
    _mock_context.get_pagerduty_integration_key = "test_integration_key"

    # Test with ignored rules failure message
    _config_args = {"message": "Spark expectations notification on rules which action_if_failed are set to ignore"}

    with patch.object(requests, "post") as mock_post:
        pagerduty_handler.send_notification(_context=_mock_context, _config_args=_config_args)
        # Should NOT call post since this is just an informational notification about ignored rules
        mock_post.assert_not_called()


def test_is_failure_notification():
    """Test the _is_failure_notification method with various message types"""
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    
    # Test failure messages that should return True
    failure_messages = [
        "Spark expectations job has been failed",
        "Job FAILED due to error",
        "Spark expectations - dropped error percentage has been exceeded above the threshold",
        "Something went wrong and FAILED",
    ]
    
    for message in failure_messages:
        assert pagerduty_handler._is_failure_notification(message) is True, f"Should detect failure in: {message}"
    
    # Test non-failure messages that should return False
    # NOTE: Ignored rules notifications are informational only and should NOT create PagerDuty incidents
    non_failure_messages = [
        "Spark expectations job has started",
        "Spark expectations job has been completed",
        "Spark expectations notification on rules which action_if_failed are set to ignore",  # Informational only
        "Everything is working fine",
        "Job succeeded",
        "Processing data successfully",
        "",
        None,
    ]
    
    for message in non_failure_messages:
        assert pagerduty_handler._is_failure_notification(message) is False, f"Should not detect failure in: {message}"
