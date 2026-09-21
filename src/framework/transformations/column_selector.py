"""Applies the source -> destination column mapping defined in datasets_columns."""
from typing import List

from pyspark.sql import DataFrame

from framework.config.models import DatasetColumnMetadata


def select_and_rename(df: DataFrame, columns_metadata: List[DatasetColumnMetadata], logger) -> DataFrame:
    """Select, rename and cast columns according to datasets_columns.

    Args:
        df: Source DataFrame.
        columns_metadata: Column mappings for the dataset (audit columns
            are excluded; they are added separately in audit_columns.py).
        logger: Logger used to report unmapped/missing columns.

    Returns:
        A DataFrame with only the configured columns, renamed to
        dest_name and cast to dest_type.
    """
    mapped_columns = [c for c in columns_metadata if not c.is_audit]

    source_columns_in_df = set(df.columns)
    configured_source_columns = {c.source_name for c in mapped_columns}

    missing_in_source = configured_source_columns - source_columns_in_df
    if missing_in_source:
        logger.warning(
            f"[COLUMN_SELECTOR] Columns configured in metadata but MISSING "
            f"from the source (cannot be mapped): {missing_in_source}"
        )

    not_mapped_in_metadata = source_columns_in_df - configured_source_columns
    if not_mapped_in_metadata:
        logger.info(
            f"[COLUMN_SELECTOR] Columns present in source but NOT configured "
            f"in metadata (dropped, not ingested): {not_mapped_in_metadata}"
        )

    select_exprs = []
    for col in mapped_columns:
        if col.source_name in source_columns_in_df:
            select_exprs.append(df[col.source_name].cast(col.dest_type).alias(col.dest_name))

    n_configured = len(mapped_columns)
    n_selected = len(select_exprs)
    logger.info(f"[COLUMN_SELECTOR] {n_selected}/{n_configured} configured columns selected from source")

    return df.select(*select_exprs)
