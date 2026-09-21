"""Basic row/null profiling for a DataFrame, logged without stopping the pipeline."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def profile_dataframe(df: DataFrame, logger) -> dict:
    """Log row count and null counts per column for a DataFrame.

    Args:
        df: DataFrame to profile.
        logger: Logger used to report the results.

    Returns:
        A dict with "row_count" and "null_counts" (column -> null count).
    """
    row_count = df.count()
    logger.info(f"[PROFILER] rows read: {row_count}")

    null_counts = {}
    if row_count > 0:
        agg_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]
        null_row = df.agg(*agg_exprs).collect()[0].asDict()
        null_counts = null_row
        for col_name, n_nulls in null_counts.items():
            if n_nulls and n_nulls > 0:
                logger.info(f"[PROFILER] column '{col_name}' has {n_nulls} nulls")

    return {"row_count": row_count, "null_counts": null_counts}
