from dataclasses import dataclass
from typing import Any, List, Dict
from pyspark.sql import DataFrame
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter
from spark_expectations.core.context import SparkExpectationsContext


@dataclass
class SparkExpectationsCollectStatistics:
    """
    This class implements logging statistics on success and failure
    """

    _context: SparkExpectationsContext
    _writer: SparkExpectationsWriter

    def __post_init__(self) -> None:
        self.collect_stats_decorator = self.collect_stats_on_success_failure()

    def collect_stats_on_success_failure(self) -> Any:
        """
        The function implements decorator to log statistics on success and failure
        Returns:
            Any: function
        """

        def decorator(func: Any) -> Any:
            def wrapper(*args: List, **kwargs: Dict) -> DataFrame:
                try:
                    self._context.set_dq_start_time()

                    result = func(*args, **kwargs)

                    self._context.set_dq_run_status("Passed")
                    self._context.set_dq_end_time()

                    self._writer.write_error_stats()
                except Exception as e:
                    self._context.set_dq_run_status("Failed")
                    self._context.set_end_time_when_dq_job_fails()
                    self._context.set_dq_end_time()

                    self._writer.write_error_stats()
                    raise SparkExpectationsMiscException(e)
                return result

            return wrapper

        return decorator
