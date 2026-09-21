"""Hashes columns marked is_sensitive=Y in datasets_columns. Runs before renaming, on source_name columns."""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.config.models import DatasetColumnMetadata


def hash_sensitive_columns(df: DataFrame, columns_metadata: List[DatasetColumnMetadata], logger) -> DataFrame:
    """Apply SHA-256 hashing to columns flagged as sensitive.

    Args:
        df: Source DataFrame (source_name columns, not yet renamed).
        columns_metadata: Column mappings for the dataset.
        logger: Logger used to report which columns were hashed.

    Returns:
        df with each sensitive column replaced by its SHA-256 hash.
    """
    sensitive_columns = [c.source_name for c in columns_metadata if c.is_sensitive]

    if not sensitive_columns:
        return df

    logger.info(f"[HASHING] applying SHA-256 hash to sensitive columns: {sensitive_columns}")

    for col_name in sensitive_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))

    return df
