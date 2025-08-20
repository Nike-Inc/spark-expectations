from spark_expectations.notifications.plugins.base_notification import SparkExpectationsNotification


def test_send_notification(mocker):
    # Create an instance of the class that implements the send_notification method
    notification_obj = SparkExpectationsNotification()
    mock_context = mocker.MagicMock()

    # Prepare test data
    config_args = {"to": "test@example.com", "subject": "Test subject", "body": "Test body"}

    # Call the send_notification method and assert that it does not return any value
    assert notification_obj.send_notification(_context=mock_context, _config_args=config_args) is None
