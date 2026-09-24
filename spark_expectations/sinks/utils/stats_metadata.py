"""
 Shared helpers for shaping the stats DataFrame before publishing metric events.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, schema_of_json


def apply_se_job_metadata_struct(df: DataFrame) -> DataFrame:
    """Convert the ``se_job_metadata`` JSON string column to a struct column, if present to prevent double-escaped string.
    Args:
        df: DataFrame to process
    
    Returns:
        DataFrame with the ``se_job_metadata`` column converted to a struct column
    """
    if "se_job_metadata" not in df.columns:
        return df

    metadata_sample = df.select("se_job_metadata").first()[0]
    if not metadata_sample:
        return df

    metadata_schema = schema_of_json(lit(metadata_sample))
    return df.withColumn("se_job_metadata", from_json(col("se_job_metadata"), metadata_schema))
