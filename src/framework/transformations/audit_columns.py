"""Adds audit columns (is_audit=Y in metadata) populated from job_timestamp."""
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.config.job_config import JobConfig
from framework.config.models import DatasetColumnMetadata

_SUPPORTED_AUDIT_COLUMNS = {
    "dt_load": lambda job_config: F.lit(job_config.job_timestamp.date().isoformat()).cast("date"),
    "ts_load": lambda job_config: F.lit(job_config.job_timestamp.isoformat()).cast("timestamp"),
}


def add_audit_columns(df: DataFrame, columns_metadata: List[DatasetColumnMetadata], job_config: JobConfig, logger) -> DataFrame:
    """Add each configured audit column with its value derived from job_timestamp.

    Args:
        df: DataFrame to add audit columns to.
        columns_metadata: Column mappings for the dataset (only is_audit=Y
            entries are used).
        job_config: Current job configuration, source of job_timestamp.
        logger: Logger used to report unsupported audit columns.

    Returns:
        df with the supported audit columns added.
    """
    audit_columns = [c for c in columns_metadata if c.is_audit]

    for col in audit_columns:
        if col.dest_name not in _SUPPORTED_AUDIT_COLUMNS:
            logger.warning(
                f"[AUDIT] audit column '{col.dest_name}' configured in metadata "
                f"but not implemented in audit_columns.py; skipping"
            )
            continue
        df = df.withColumn(col.dest_name, _SUPPORTED_AUDIT_COLUMNS[col.dest_name](job_config))

    return df
