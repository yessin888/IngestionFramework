"""Writer for the raw (bronze) layer, in Delta format.

type_load=FULL overwrites raw_path; type_load=INCREMENTAL appends the
rows already filtered by the caller. Physically partitions by
partition_field when configured.
"""
from pyspark.sql import DataFrame

from framework.config.models import DatasetMetadata
from framework.writers.base_writer import BaseWriter


class RawWriter(BaseWriter):
    """Writes a dataset into its raw_path as a Delta table."""

    def __init__(self, file_format: str = "delta"):
        """Initialize the writer.

        Args:
            file_format: Spark write format, "delta" by default.
        """
        self.file_format = file_format

    def write(self, df: DataFrame, dataset: DatasetMetadata, logger) -> None:
        """Write df into dataset.raw_path.

        Args:
            df: DataFrame to write.
            dataset: Metadata of the dataset being written.
            logger: Logger used to report the write.

        Raises:
            ValueError: If type_load is not FULL or INCREMENTAL.
        """
        writer = df.write.format(self.file_format)

        if dataset.partition_field and dataset.partition_field in df.columns:
            writer = writer.partitionBy(dataset.partition_field)

        if dataset.type_load.upper() == "FULL":
            mode = "overwrite"
        elif dataset.type_load.upper() == "INCREMENTAL":
            mode = "append"
        else:
            raise ValueError(f"type_load '{dataset.type_load}' is not supported by RawWriter")

        logger.info(
            f"[WRITE] writing to raw_path='{dataset.raw_path}' "
            f"format={self.file_format} mode={mode} "
            f"partition_field={dataset.partition_field}"
        )

        writer.mode(mode).save(dataset.raw_path)
