from unittest.mock import patch, Mock
import pytest
import requests
from spark_expectations.core.exceptions import SparkExpectationsSlackNotificationException
from spark_expectations.notifications.plugins.slack import SparkExpectationsSlackPluginImpl


@patch("spark_expectations.notifications.plugins.slack.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_success(_mock_context):
    # Arrange
    slack_handler = SparkExpectationsSlackPluginImpl()
    _mock_context.get_enable_slack = True
    _mock_context.get_slack_webhook_url = "http://test_webhook_url"

    _config_args = {"message": "test message"}

    # Mock requests.post to return a response with status code 200
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # Act
        slack_handler.send_notification(_context=_mock_context, _config_args=_config_args)

        # Assert
        mock_post.assert_called_once_with(
            _mock_context.get_slack_webhook_url, json={"text": "test message"}, timeout=10
        )


@patch("spark_expectations.notifications.plugins.slack.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_exception(_mock_context):
    # Arrange
    slack_handler = SparkExpectationsSlackPluginImpl()
    _mock_context.get_enable_slack = True
    _mock_context.get_slack_webhook_url = "http://test_webhook_url"
    _config_args = {"message": "test message"}

    # Mock requests.post to return a response with status code 404
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        # Act and Assert
        with pytest.raises(SparkExpectationsSlackNotificationException):
            slack_handler.send_notification(_context=_mock_context, _config_args=_config_args)


@patch("spark_expectations.notifications.plugins.slack.SparkExpectationsContext", autospec=True, spec_set=True)
def test_send_notification_slack_disabled(_mock_context):
    # Arrange
    slack_handler = SparkExpectationsSlackPluginImpl()
    _mock_context.get_enable_slack = False
    _mock_context.get_slack_webhook_url = "http://test_webhook_url"
    _config_args = {"message": "test message"}

    with patch.object(requests, "post") as mock_post:
        # Act
        slack_handler.send_notification(_context=_mock_context, _config_args=_config_args)

        mock_post.post.assert_not_called()
