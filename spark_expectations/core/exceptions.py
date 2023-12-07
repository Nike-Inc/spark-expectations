# from pyspark.sql.utils import CapturedException


class SparkExpectationsDataframeNotReturnedException(Exception):
    """
    Throw this exception if a function doesn't return a dataframe
    """

    pass


class SparkExpectOrFailException(Exception):
    """
    Throw this exception if a rule fails and is expected to fail the job
    """

    pass


class SparkExpectationsUserInputOrConfigInvalidException(Exception):
    """
    Throw this exception when configured rule or value from the user is wrong
    """

    pass


# class SparkExpectationsSqlQueryParserException(CapturedException):
#     """
#     Throw this exception when spark expectations fail to parse the given user query
#     """
#
#     pass


class SparkExpectationsMiscException(Exception):
    """
    Throw this exception when spark expectations encounters miscellaneous exceptions
    """

    pass


class SparkExpectationsSlackNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending Slack notifications
    """

    pass


class SparkExpectationsTeamsNotificationException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending Teams notifications
    """

    pass


class SparkExpectationsEmailException(Exception):
    """
    Throw this exception when spark expectations encounters exceptions while sending email notifications
    """

    pass


class SparkExpectationsErrorThresholdExceedsException(Exception):
    """
    Throw this exception when error percentage exceeds certain configured value
    """

    pass
