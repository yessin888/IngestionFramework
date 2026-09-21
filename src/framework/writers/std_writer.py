"""Writer for the std (silver) layer, in Delta format.

type_load=FULL overwrites std_path. type_load=INCREMENTAL runs a MERGE
by business key (is_business_key in datasets_columns): UPDATE if the
key already exists, INSERT otherwise. If std_path is not a Delta table
yet, an overwrite bootstraps it instead of attempting a MERGE.
"""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from framework.config.models import DatasetMetadata
from framework.writers.base_writer import BaseWriter


class StdWriter(BaseWriter):
    """Writes a dataset into its std_path as a Delta table, via overwrite or MERGE."""

    def __init__(self, file_format: str = "delta"):
        """Initialize the writer.

        Args:
            file_format: Spark write format, "delta" by default.
        """
        self.file_format = file_format

    def write(self, df: DataFrame, dataset: DatasetMetadata, logger) -> None:
        """Write df into dataset.std_path.

        Args:
            df: DataFrame to write (already renamed to dest_name columns).
            dataset: Metadata of the dataset being written.
            logger: Logger used to report the write.

        Raises:
            ValueError: If no is_business_key column is configured.
        """
        key_columns = [c.dest_name for c in dataset.business_key_columns()]
        if not key_columns:
            raise ValueError(
                f"dataset_id={dataset.dataset_id} has no is_business_key=Y column "
                f"in datasets_columns; StdWriter needs a business key to MERGE into std"
            )

        is_full = dataset.type_load.upper() == "FULL"
        table_exists = DeltaTable.isDeltaTable(df.sparkSession, dataset.std_path)

        if is_full or not table_exists:
            logger.info(f"[WRITE] std_path='{dataset.std_path}' format={self.file_format} mode=overwrite")
            df.write.format(self.file_format).mode("overwrite").save(dataset.std_path)
            return

        condition = " AND ".join(f"target.{c} = source.{c}" for c in key_columns)

        logger.info(f"[WRITE] MERGE into std_path='{dataset.std_path}' format={self.file_format} ON ({condition})")

        (
            DeltaTable.forPath(df.sparkSession, dataset.std_path).alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
