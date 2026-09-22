"""Incremental filtering and watermark derivation for INCREMENTAL datasets.

The watermark for a layer is derived on demand by reading the maximum
incremental_field already present in that layer's Delta table
(get_current_watermark), instead of being persisted separately.
"""
from datetime import datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from framework.config.models import DatasetMetadata


def apply_incremental_filter(df: DataFrame, dataset: DatasetMetadata, watermark: Optional[str], logger) -> DataFrame:
    """Filter df to rows where incremental_field is greater than watermark.

    Args:
        df: DataFrame to filter (must contain dataset.incremental_field).
        dataset: Metadata of the dataset being filtered.
        watermark: ISO datetime string, or None to skip filtering.
        logger: Logger used to report the filter applied.

    Returns:
        df unfiltered if watermark is None, otherwise the rows newer than watermark.

    Raises:
        ValueError: If incremental_field or incremental_mask is not configured.
    """
    if not dataset.incremental_field:
        raise ValueError(
            f"dataset_id={dataset.dataset_id} has type_load=INCREMENTAL but no "
            f"incremental_field configured in metadata"
        )
    if not dataset.incremental_mask:
        raise ValueError(
            f"dataset_id={dataset.dataset_id} has type_load=INCREMENTAL but no "
            f"incremental_mask configured in metadata"
        )

    if watermark is None:
        logger.info(f"[INCREMENTAL] dataset_id={dataset.dataset_id} has no previous watermark -> full initial load")
        return df

    watermark_dt = datetime.fromisoformat(watermark)
    logger.info(f"[INCREMENTAL] filtering '{dataset.incremental_field}' (mask={dataset.incremental_mask}) > {watermark}")
    # TODO alinear el todo de abajo con esto
    return (
        df.withColumn("_incremental_field_parsed", F.to_timestamp(F.col(dataset.incremental_field), dataset.incremental_mask))
        .filter(F.col("_incremental_field_parsed") > F.lit(watermark_dt))
        .drop("_incremental_field_parsed")
    )


def get_current_watermark(spark: SparkSession, path: str, dataset: DatasetMetadata, field_name: str) -> Optional[str]:
    """Derive the current watermark for a layer from its Delta table.

    Args:
        spark: Active SparkSession.
        path: Path of the layer's Delta table.
        dataset: Metadata of the dataset being processed.
        field_name: Name of the incremental field as it appears at path
            (source_name for raw, dest_name for std).

    Returns:
        The maximum field_name value (parsed with incremental_mask) in ISO
        format, or None if the table does not exist yet.

    Raises:
        ValueError: If incremental_field or incremental_mask is not configured.
    """
    if not dataset.incremental_field:
        raise ValueError(
            f"dataset_id={dataset.dataset_id} has type_load=INCREMENTAL but no "
            f"incremental_field configured in metadata"
        )
    if not dataset.incremental_mask:
        raise ValueError(
            f"dataset_id={dataset.dataset_id} has type_load=INCREMENTAL but no "
            f"incremental_mask configured in metadata"
        )

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    # TODO revisar si puedo hacer que se aprovechen estadísticas Delta para obtener el max_value más eficientemente. Es decir, comprobar si por hacer el cast se está leyendo todo los archivos
    max_value = (
        spark.read.format("delta").load(path)
        .agg(F.max(F.to_timestamp(F.col(field_name), dataset.incremental_mask)).alias("max_v"))
        .collect()[0]["max_v"]
    )
    return max_value.isoformat() if max_value is not None else None
