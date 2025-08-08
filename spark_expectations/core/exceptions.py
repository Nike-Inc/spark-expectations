class SparkExpectationsDataframeNotReturnedException(Exception):
    """
    Throw this exception if a function doesn't return a dataframe
    """


class SparkExpectOrFailException(Exception):
    """
    Throw this exception if a rule fails and is expected to fail the job
    """


class SparkExpectationsUserInputOrConfigInvalidException(Exception):
    """
    Throw this exception when configured rule or value from the user is wrong
    """


class SparkExpectationsMiscException(Exception):
    """
    Throw this exception when spark expectations encounters miscellaneous exceptions
    """


class SparkExpectationsSlackNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending Slack notifications
    """


class SparkExpectationsTeamsNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending Teams notifications
    """


class SparkExpectationsZoomNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending Zoom notifications
    """

class SparkExpectationsPagerDutyNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending PagerDuty API notifications
    """

class SparkExpectationsEmailException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending email notifications
    """


class SparkExpectationsErrorThresholdExceedsException(Exception):
    """
    Throw this exception when error percentage exceeds certain configured value
    """
