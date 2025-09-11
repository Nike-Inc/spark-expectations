from unittest.mock import patch, Mock
import pytest
import requests
from spark_expectations.core.exceptions import SparkExpectationsPagerDutyException
from spark_expectations.notifications.plugins.pagerduty import SparkExpectationsPagerDutyPluginImpl


@patch("spark_expectations.notifications.plugins.pagerduty.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_incident_success(_mock_context):
    pagerduty_handler = SparkExpectationsPagerDutyPluginImpl()
    _mock_context.get_enable_pagerduty = True
    _mock_context.get_pagerduty_webhook_url = "http://test_webhook_url"

    _config_args = {"message": "test message"}

    # Mock requests.post to return a response with status code 200
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 202
        mock_post.return_value = mock_response

        pagerduty_handler.create_incident(_context=_mock_context, _config_args=_config_args)

        mock_post.assert_called_once_with(
            _mock_context.get_pagerduty_webhook_url,
            json={
                "routing_key": _mock_context.get_pagerduty_integration_key,
                "event_action": "trigger",
                "payload": {
                    "summary": "test message",
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
    _config_args = {"message": "test message"}

    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        with pytest.raises(SparkExpectationsPagerDutyException):
            pagerduty_handler.create_incident(_context=_mock_context, _config_args=_config_args)


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
        pagerduty_handler.create_incident(_context=_mock_context, _config_args=_config_args)
        mock_post.post.assert_not_called()
