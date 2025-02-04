from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.config.user_config import Constants as user_config
import os
from typing import Optional, Union, Dict, Tuple
from dataclasses import dataclass
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, when, max
from spark_expectations import _log
from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.config.user_config import Constants as user_config

from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
)



@dataclass
class SparkExpectationsReadercall:
    """
    This class implements/supports reading data from source system
    """

    _context: SparkExpectationsContext

    def __post_init__(self) -> None:
        self.spark = self._context.spark

    def set_notification_param2(


        self, notification: Optional[Dict[str, Union[int, str, bool]]] = None
    ) -> None:
        _notification_dict: Dict[str, Union[str, int, bool]] = (
             notification


        )





        print(_notification_dict.get(user_config.se_enable_obs_dq_report_result))