"""Raw -> std (silver) job.

For each dataset: derive the std-layer watermark and filter raw_path
(Delta) to only the new rows (or read it all, for FULL datasets or the
first run), profile, rename/cast to dest_name/dest_type, add audit
columns, and write to std via a business-key MERGE (StdWriter).
"""
from framework.core.logger import get_logger
from framework.core.profiler import profile_dataframe
from framework.jobs.base_job import BaseJob
from framework.transformations.audit_columns import add_audit_columns
from framework.transformations.column_selector import select_and_rename
from framework.transformations.incremental import apply_incremental_filter, get_current_watermark
from framework.writers.std_writer import StdWriter


class RawToStdJob(BaseJob):
    """Orchestrates reading from raw and writing into the std layer."""

    def process(self) -> None:
        """Process every dataset_id configured for this job run."""
        datasets_metadata = self.metadata_repository.get_datasets(
            dataset_ids=self.job_config.dataset_ids
        )

        for dataset_metadata in datasets_metadata:
            self._process_dataset(dataset_metadata)

    def _process_dataset(self, dataset_metadata) -> None:
        """Read, transform and merge a single dataset into std.

        Args:
            dataset_metadata: Metadata of the dataset to process.
        """
        logger = get_logger(self.job_config.run_id, dataset_metadata.dataset_id)
        logger.info(f"[RAW_STD] starting dataset '{dataset_metadata.tablename}' (raw_path={dataset_metadata.raw_path})")

        df_raw = self.spark.read.format("delta").load(dataset_metadata.raw_path)

        if dataset_metadata.type_load.upper() == "INCREMENTAL":
            std_field = dataset_metadata.dest_name_for(dataset_metadata.incremental_field)
            watermark = get_current_watermark(self.spark, dataset_metadata.std_path, dataset_metadata, std_field)
            df_raw = apply_incremental_filter(df_raw, dataset_metadata, watermark, logger)

        profile_dataframe(df_raw, logger)

        df_selected = select_and_rename(df_raw, dataset_metadata.columns, logger)

        df_final = add_audit_columns(df_selected, dataset_metadata.columns, self.job_config, logger)

        writer = StdWriter(file_format="delta")
        writer.write(df_final, dataset_metadata, logger)

        logger.info(f"[RAW_STD] dataset '{dataset_metadata.tablename}' completed OK -> {dataset_metadata.std_path}")
