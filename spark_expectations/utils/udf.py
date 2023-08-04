from typing import List, Dict, Union
from pyspark.sql import Column
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, MapType, StringType


@udf(returnType=ArrayType(MapType(StringType(), StringType())))
def remove_empty_maps(column: Column) -> List[Union[Dict[str, str], None]]:
    """
    This Spark UDF takes a column of type array(map(str,str)) and removes empty maps from it
    Args:
        column: Provide a column of type array(map(str,str))
        Returns:
                list: Returns a list which is not having empty maps
    """
    # The below line is already tested in test_udf.py but not shown in coverage. So ignoring it for now!
    return [
        x for x in column if isinstance(x, dict) and len(x) != 0
    ]  # pragma: no cover


@udf(returnType=ArrayType(StringType()))
def get_actions_list(column: Column) -> List[str]:
    """
    This Spark UDF takes column of type array(map(str,str)) and creates list by picking action_if_failed from dict
    Args:
        column: Provide a column of type array(map(str,str))

    Returns:
           list: returns list of action_if_failed from the set expectations rules

    """

    action_failed = [itr.get("action_if_failed") for itr in column]  # pragma: no cover

    return action_failed if len(action_failed) > 0 else ["ignore"]  # pragma: no cover
