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


class SparkExpectationsPagerDutyException(Exception):
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


class SparkExpectationsInvalidRuleTypeException(Exception):
    """
    Throw this exception when an invalid rule type is encountered
    """


class SparkExpectationsInvalidRowDQExpectationException(Exception):
    """
    Throw this exception when an invalid row_dq expectation is encountered
    """


class SparkExpectationsInvalidQueryDQExpectationException(Exception):
    """
    Throw this exception when an invalid query_dq expectation is encountered
    """


class SparkExpectationsInvalidAggDQExpectationException(Exception):
    """
    Throw this exception when an invalid agg_dq expectation is encountered
    """

def raise_if_ansi_exception(e: Exception, rule_name: str, rule_expectation: str) -> None:
    """Check if exception is likely cast error due to ANSI mode. Add that info to error message and raise."""
    if "CAST_INVALID_INPUT" in str(e):
        raise SparkExpectationsMiscException(
            f"Cast error while evaluating rule '{rule_name}' with expectation/SQL '{rule_expectation}'. "
            f"This may be caused by rule expectation/SQL incompatibility with spark.sql.ansi.enabled=true "
            f"(default on Databricks Serverless). Please either update your expectation/SQL "
            f"to be compliant with ANSI mode, disable ANSI mode, or run not on serverless."
        ) from e
