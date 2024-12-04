from dataclasses import dataclass
from typing import Any, List, Dict
from pyspark.sql import DataFrame
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.notifications import _notification_hook
from spark_expectations.core.exceptions import SparkExpectationsMiscException


@dataclass
class SparkExpectationsNotify:
    """
    This class implements Notification
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.send_notification_decorator: Any = self.notify_on_start_completion_failure(
            self.notify_on_start, self.notify_on_completion, self.notify_on_failure
        )

    def notify_on_start_completion_failure(
        self, _on_start: Any, _on_completion: Any, _on_failure: Any
    ) -> Any:
        """
        This function orchestrate notification
        Args:
            _on_start: function to send notification on start of spark expectations
            _on_completion: function to send notification on completion of spark expectations
            _on_failure: function to send notification on failure

        Returns: decorated notification function

        """

        def decorator(func: Any) -> Any:
            def wrapper(*args: List, **kwargs: Dict) -> DataFrame:
                if self._context.get_notification_on_start is True:
                    _on_start()
                try:
                    # self._context.set_dq_start_time()
                    result = func(*args, **kwargs)

                    if self._context.get_notification_on_completion is True:
                        _on_completion()
                    # self._context.set_dq_end_time()

                except Exception as e:
                    # self._context.set_dq_run_status("Failed")
                    if self._context.get_notification_on_fail is True:
                        _on_failure(e)
                    # self._context.set_dq_end_time()
                    raise SparkExpectationsMiscException(e)
                return result

            return wrapper

        return decorator

    def notify_on_start(self) -> None:
        """
        This function sends notification on start of spark expectations project
        Returns: None

        """

        _notification_message = (
            "Spark expectations job has started \n\n"
            f"table_name: {self._context.get_table_name}\n"
            f"run_id: {self._context.get_run_id}\n"
            f"run_date: {self._context.get_run_date}"
        )

        _notification_hook.send_notification(
            _context=self._context,
            _config_args={
                "message": _notification_message,
            },
        )

    def notify_on_exceeds_of_error_threshold(self) -> None:
        """
        This function sends notification on completion of spark expectations project
        Returns: None

        """

        _notification_message = (
            f"Spark expectations - dropped error percentage has been exceeded above the threshold "
            f"value({self._context.get_error_drop_threshold}%) for `row_data` quality validation  \n\n"
            f"product_id: {self._context.product_id}\n"
            f"table_name: {self._context.get_table_name}\n"
            f"run_id: {self._context.get_run_id}\n"
            f"run_date: {self._context.get_run_date}\n"
            f"input_count: {self._context.get_input_count}\n"
            f"error_percentage: {self._context.get_error_percentage}\n"
            f"error_drop_percentage: {self._context.get_error_drop_percentage}\n"
            f"output_percentage: {self._context.get_output_percentage}\n"
            f"success_percentage: {self._context.get_success_percentage}"
            # f"status: source_agg_dq_status = {self._context.get_source_agg_dq_status}\n"
            # f"            source_query_dq_status = {self._context.get_source_query_dq_status}\n"
            # f"            row_dq_status = {self._context.get_row_dq_status}\n"
            # f"            final_agg_dq_status = {self._context.get_final_agg_dq_status}\n"
            # f"            final_query_dq_status = {self._context.get_final_query_dq_status}\n"
            # f"            run_status = {self._context.get_dq_run_status}"
        )

        _notification_hook.send_notification(
            _context=self._context, _config_args={"message": _notification_message}
        )

    def notify_on_completion(self) -> None:
        """
        This function sends notification on completion of spark expectations project
        Returns: None

        """

        _notification_message = (
            "Spark expectations job has been completed  \n\n"
            f"product_id: {self._context.product_id}\n"
            f"table_name: {self._context.get_table_name}\n"
            f"run_id: {self._context.get_run_id}\n"
            f"run_date: {self._context.get_run_date}\n"
            f"input_count: {self._context.get_input_count}\n"
            f"error_percentage: {self._context.get_error_percentage}\n"
            f"output_percentage: {self._context.get_output_percentage}\n"
            f"success_percentage: {self._context.get_success_percentage}\n"
            f"status: source_agg_dq_status = {self._context.get_source_agg_dq_status}\n"
            f"            source_query_dq_status = {self._context.get_source_query_dq_status}\n"
            f"            row_dq_status = {self._context.get_row_dq_status}\n"
            f"            final_agg_dq_status = {self._context.get_final_agg_dq_status}\n"
            f"            final_query_dq_status = {self._context.get_final_query_dq_status}\n"
            f"            run_status = {self._context.get_dq_run_status}"
        )

        _notification_hook.send_notification(
            _context=self._context, _config_args={"message": _notification_message}
        )

    def notify_on_failure(self, _error: str) -> None:
        """
        This function sends notification on failure of spark expectations project
        Args:
            _error: message or exception  for the failure(str)

        Returns: None

        """

        _notification_message = (
            "Spark expectations job has been failed  \n\n"
            f"product_id: {self._context.product_id}\n"
            f"table_name: {self._context.get_table_name}\n"
            f"run_id: {self._context.get_run_id}\n"
            f"run_date: {self._context.get_run_date}\n"
            f"input_count: {self._context.get_input_count}\n"
            f"error_percentage: {self._context.get_error_percentage}\n"
            f"output_percentage: {self._context.get_output_percentage}\n"
            f"status: source_agg_dq_status = {self._context.get_source_agg_dq_status}\n"
            f"            source_query_dq_status = {self._context.get_source_query_dq_status}\n"
            f"            row_dq_status = {self._context.get_row_dq_status}\n"
            f"            final_agg_dq_status = {self._context.get_final_agg_dq_status}\n"
            f"            final_query_dq_status = {self._context.get_final_query_dq_status}\n"
            f"            run_status = {self._context.get_dq_run_status}"
        )

        _notification_hook.send_notification(
            _context=self._context,
            _config_args={
                "message": _notification_message,
            },
        )

    def construct_message_for_each_rules(
        self,
        rule_name: str,
        failed_row_count: int,
        error_drop_percentage: float,
        set_error_drop_threshold: float,
        action: str,
    ) -> str:
        """
        This function supports constructing the notification message when rule threshold exceeds certain threshold
         Args:
            rule_name: name of the dq rule
            failed_row_count: number of failed of dq rule
            error_drop_percentage: error drop percentage
         Returns: str
        """

        _notification_message = (
            f"{rule_name} has been exceeded above the threshold "
            f"value({set_error_drop_threshold}%) for `row_data` quality validation\n"
            f"product_id: {self._context.product_id}\n"
            f"table_name: {self._context.get_table_name}\n"
            f"run_id: {self._context.get_run_id}\n"
            f"run_date: {self._context.get_run_date}\n"
            f"input_count: {self._context.get_input_count}\n"
            f"rule_name: {rule_name}\n"
            f"action: {action}\n"
            f"failed_row_count: {failed_row_count}\n"
            f"error_drop_percentage: {error_drop_percentage}\n\n\n"
        )

        return _notification_message

    def notify_on_exceeds_of_error_threshold_each_rules(
        self,
        message: str,
    ) -> None:
        """
        This function sends notification when specific rule error drop percentage exceeds above threshold
        Args:
            message: message to be sent in notification
        Returns: None

        """

        _notification_message = (
            f"Spark expectations - The number of notifications for rules being followed has surpassed "
            f"the specified threshold \n\n\n{message}"
        )

        _notification_hook.send_notification(
            _context=self._context, _config_args={"message": _notification_message}
        )

    def notify_rules_exceeds_threshold(self, rules: dict) -> None:
        """
        This functions identifies error drop percentage for rules which exceeds above set threshold
        Args:
            rules: lsit of rules which set to do data quality checks
        Returns:
            None

        """
        try:
            rules_failed_row_count: Dict[str, int] = {}
            notification_body = ""
            if self._context.get_summarized_row_dq_res is None:
                return None

            rules_failed_row_count = {
                itr["rule"]: int(itr["failed_row_count"])
                for itr in self._context.get_summarized_row_dq_res
            }

            for rule in rules[f"{self._context.get_row_dq_rule_type_name}_rules"]:
                if not rule["enable_error_drop_alert"]:
                    continue

                rule_name = rule["rule"]
                rule_action = rule["action_if_failed"]
                failed_row_count = int(
                    rules_failed_row_count[rule_name]
                    if rule_name in rules_failed_row_count
                    else 0
                )

                if failed_row_count is not None and failed_row_count > 0:
                    set_error_drop_threshold = int(rule["error_drop_threshold"])
                    error_drop_percentage = round(
                        (failed_row_count / self._context.get_input_count) * 100, 2
                    )

                    if error_drop_percentage >= set_error_drop_threshold:
                        notification_body = (
                            notification_body
                            + self.construct_message_for_each_rules(
                                rule_name=rule_name,
                                failed_row_count=failed_row_count,
                                error_drop_percentage=error_drop_percentage,
                                set_error_drop_threshold=set_error_drop_threshold,
                                action=rule_action,
                            )
                        )
                if notification_body != "":
                    self.notify_on_exceeds_of_error_threshold_each_rules(
                        notification_body
                    )

        except Exception as e:
            raise SparkExpectationsMiscException(
                f"An error occurred while sending notification when the error threshold is breached: {e}"
            )
