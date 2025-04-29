from pyspark.sql import Column
from pyspark.sql.functions import filter, size, transform, when, lit, array


def remove_empty_maps(column: Column) -> Column:
    """
    This function takes a column of type array(map(str,str)) and removes empty maps from it
    Args:
        column: Provide a column of type array(map(str,str))
        Returns:
                list: Returns a Column which is not having empty maps
    """
    # The below line is already tested in test_udf.py but not shown in coverage. So ignoring it for now!
    return filter(column, lambda x: size(x) > 0)  # pragma: no cover


def remove_passing_status_maps(column: Column) -> Column:
    """
    This function takes a column of type array(map(str,str)) and removes maps with passing status from it
    Args:
        column: Provide a column of type array(map(str,str))

    Returns:
           list: returns a Column with items with a passing status removed.

    """
    return filter(column, lambda x: x.getItem("status") != "pass")  # pragma: no cover


def get_actions_list(column: Column) -> Column:
    """
    This function takes column of type array(map(str,str)) and creates list by picking action_if_failed from dict of failed expectations rules.
    Args:
        column: Provide a column of type array(map(str,str))

    Returns:
           list: returns a column with list of action_if_failed from the set expectations rules

    """
    column = remove_passing_status_maps(column)
    action_if_failed = transform(column, lambda x: x["action_if_failed"])
    return when(size(action_if_failed) == 0, array(lit("ignore"))).otherwise(action_if_failed)  # pragma: no cover
