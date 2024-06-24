from unittest.mock import patch, Mock
import pytest
import requests
from spark_expectations.core.exceptions import SparkExpectationsZoomNotificationException
from spark_expectations.notifications.plugins.zoom import SparkExpectationsZoomPluginImpl


@patch('spark_expectations.notifications.plugins.zoom.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_success(_mock_context):
    # Arrange
    zoom_handler = SparkExpectationsZoomPluginImpl()
    _mock_context.get_enable_zoom = True
    _mock_context.get_zoom_webhook_url = "http://test_webhook_url"
    _mock_context.get_zoom_token = "abcdefghi"

    _config_args = {
        "title": "SE Notification",
        "themeColor": "008000", "message": "test message"}

    # Mock requests.post to return a response with status code 200
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # Act
        zoom_handler.send_notification(_context=_mock_context, _config_args=_config_args)

        # Assert
        mock_post.assert_called_once_with(_mock_context.get_zoom_webhook_url, json={
            "title": "SE Notification",
            "themeColor": "008000", "text": "test message"}, headers={"Authorization": "Bearer abcdefghi", "Content-Type": "application/json"}, timeout=10)



@patch('spark_expectations.notifications.plugins.zoom.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_exception(_mock_context):
    # Arrange
    zoom_handler = SparkExpectationsZoomPluginImpl()
    _mock_context.get_enable_zoom = True
    _mock_context.get_zoom_webhook_url = "http://test_webhook_url"
    _mock_context.get_zoom_token = "abcdefghi"
    _config_args = {"message": "test message"}

    # Mock requests.post to return a response with status code 404
    with patch.object(requests, "post") as mock_post:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_post.return_value = mock_response

        # Act and Assert
        with pytest.raises(SparkExpectationsZoomNotificationException):
            zoom_handler.send_notification(_context=_mock_context, _config_args=_config_args)


@patch('spark_expectations.notifications.plugins.zoom.SparkExpectationsContext', autospec=True, spec_set=True)
def test_send_notification_zoom_disabled(_mock_context):
    # Arrange
    zoom_handler = SparkExpectationsZoomPluginImpl()
    _mock_context.get_enable_zoom = False
    _mock_context.get_zoom_webhook_url = "http://test_webhook_url"
    _mock_context.get_zoom_token = "abcdefghi"
    _config_args = {"message": "test message"}

    with patch.object(requests, "post") as mock_post:
        # Act
        zoom_handler.send_notification(_context=_mock_context, _config_args=_config_args)

        mock_post.post.assert_not_called()
