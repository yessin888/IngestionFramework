"""Source -> raw (bronze) job.

For each dataset: derive the raw-layer watermark, read from the source
(full or incremental), profile, hash sensitive columns, and write to
raw_path. Bronze keeps the source column names and types as-is; the
rename/cast to dest_name/dest_type and audit columns are applied later,
in RawToStdJob.
"""
from framework.core.logger import get_logger
from framework.core.profiler import profile_dataframe
from framework.jobs.base_job import BaseJob
from framework.readers.reader_factory import ReaderFactory
from framework.transformations.hashing import hash_sensitive_columns
from framework.transformations.incremental import get_current_watermark
from framework.writers.raw_writer import RawWriter


class SourceToRawJob(BaseJob):
    """Orchestrates reading from source and writing into the raw layer."""

    def process(self) -> None:
        """Process every dataset_id configured for this job run."""
        datasets_metadata = self.metadata_repository.get_datasets(
            dataset_ids=self.job_config.dataset_ids
        )

        for dataset_metadata in datasets_metadata:
            self._process_dataset(dataset_metadata)

    def _process_dataset(self, dataset_metadata) -> None:
        """Read, hash and write a single dataset into raw.

        Args:
            dataset_metadata: Metadata of the dataset to process.
        """
        logger = get_logger(self.job_config.run_id, dataset_metadata.dataset_id)
        logger.info(
            f"[SOURCE_RAW] starting dataset '{dataset_metadata.tablename}' "
            f"(type_origin={dataset_metadata.type_origin}, subtype_origin={dataset_metadata.subtype_origin}, "
            f"type_read={dataset_metadata.type_read})"
        )

        watermark = None
        if dataset_metadata.type_load.upper() == "INCREMENTAL":
            watermark = get_current_watermark(
                self.spark, dataset_metadata.raw_path, dataset_metadata, dataset_metadata.incremental_field
            )
        reader = ReaderFactory.get_reader(self.spark, dataset_metadata.type_read)
        df_source = reader.read(dataset_metadata, self.job_config, watermark)

        profile_dataframe(df_source, logger)

        df_hashed = hash_sensitive_columns(df_source, dataset_metadata.columns, logger)

        writer = RawWriter(file_format="delta")
        writer.write(df_hashed, dataset_metadata, logger)

        logger.info(f"[SOURCE_RAW] dataset '{dataset_metadata.tablename}' completed OK -> {dataset_metadata.raw_path}")
