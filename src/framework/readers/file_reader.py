"""Reader for file-based sources landed locally (CSV/JSON/PARQUET)."""
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, _parse_datatype_string

from framework.config.job_config import JobConfig
from framework.config.models import DatasetColumnMetadata, DatasetMetadata
from framework.readers.base_reader import BaseReader
from framework.transformations.incremental import apply_incremental_filter

_SUPPORTED_FILE_TYPES = {"CSV", "JSON", "PARQUET"}


class FileReader(BaseReader):
    """Reads CSV/JSON/PARQUET files from a landing path, applying full or incremental strategy."""

    def __init__(self, spark):
        super().__init__(spark)

    def read(self, dataset_metadata: DatasetMetadata, job_config: JobConfig, watermark: Optional[str] = None) -> DataFrame:
        """Read a dataset from its source_path.

        Args:
            dataset_metadata: Metadata of the dataset to read.
            job_config: Current job configuration.
            watermark: Last watermark for this layer, or None for a full load.

        Returns:
            The DataFrame read from source_path, filtered if incremental.

        Raises:
            ValueError: If type_file is not supported.
        """
        file_type = (dataset_metadata.type_file or "CSV").upper()
        if file_type not in _SUPPORTED_FILE_TYPES:
            raise ValueError(
                f"type_file '{file_type}' is not supported by FileReader. "
                f"Supported: {_SUPPORTED_FILE_TYPES}"
            )
        # TODO ADD DELTA TYPE
        if file_type == "PARQUET":
            df = self.spark.read.parquet(dataset_metadata.source_path)
        else:
            schema = self._build_source_schema(dataset_metadata)
            reader = self.spark.read.schema(schema)
            if file_type == "JSON":
                df = reader.json(dataset_metadata.source_path)
            else:
                df = (
                    reader
                    .option("header", "true")
                    .option("enforceSchema", "false")
                    .csv(dataset_metadata.source_path)
                )

        return self._apply_read_strategy(df, dataset_metadata, job_config, watermark)

    def _build_source_schema(self, dataset_metadata: DatasetMetadata) -> StructType:
        """Build an explicit read schema from datasets_columns (source_name/source_type).

        Args:
            dataset_metadata: Metadata of the dataset to read.

        Returns:
            A StructType matching the configured source columns, in column_order.

        Raises:
            ValueError: If the dataset has no non-audit columns configured.
        """
        columns = dataset_metadata.non_audit_columns()
        if not columns:
            raise ValueError(
                f"dataset_id={dataset_metadata.dataset_id} has no columns configured "
                f"in datasets_columns; cannot build the read schema"
            )
        return StructType([
            StructField(col.source_name, self._resolve_source_type(col), True)
            for col in columns
        ])

    def _resolve_source_type(self, col: DatasetColumnMetadata):
        """Parse a column's source_type into a Spark DataType.

        Args:
            col: Column metadata whose source_type should be resolved.

        Returns:
            The corresponding pyspark.sql.types.DataType.

        Raises:
            ValueError: If source_type is not configured.
        """
        if not col.source_type:
            raise ValueError(
                f"column '{col.source_name}' (dataset_id={col.dataset_id}) has no "
                f"source_type configured in datasets_columns"
            )
        return _parse_datatype_string(col.source_type)

    def _apply_read_strategy(self, df: DataFrame, dataset_metadata: DatasetMetadata, job_config: JobConfig, watermark: Optional[str]) -> DataFrame:
        """Apply the full or incremental read strategy based on type_load.

        Args:
            df: DataFrame read from the source.
            dataset_metadata: Metadata of the dataset being read.
            job_config: Current job configuration.
            watermark: Last watermark for this layer, or None for a full load.

        Returns:
            df unchanged for FULL loads, or filtered for INCREMENTAL loads.

        Raises:
            ValueError: If type_load is not supported.
        """
        if dataset_metadata.type_load.upper() == "FULL":
            return df
        elif dataset_metadata.type_load.upper() == "INCREMENTAL":
            from framework.core.logger import get_logger
            logger = get_logger(job_config.run_id, dataset_metadata.dataset_id)
            return apply_incremental_filter(df, dataset_metadata, watermark, logger)
        else:
            raise ValueError(f"type_load '{dataset_metadata.type_load}' is not supported")
