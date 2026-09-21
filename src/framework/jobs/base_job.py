"""Template Method skeleton (init_config -> process) shared by every job."""
from abc import ABC, abstractmethod

from framework.config.job_config import JobConfig
from framework.config.metadata_repository import MetadataRepository
from framework.core.logger import get_logger


class BaseJob(ABC):
    """Base class for jobs. Subclasses implement process()."""

    def __init__(self, spark, metadata_repository: MetadataRepository, raw_job_params: dict):
        """Initialize the job.

        Args:
            spark: Active SparkSession.
            metadata_repository: Repository used to load dataset metadata.
            raw_job_params: Raw job parameters (e.g. parsed CLI arguments).
        """
        self.spark = spark
        self.metadata_repository = metadata_repository
        self.raw_job_params = raw_job_params
        self.job_config: JobConfig = None
        self.logger = get_logger(run_id=raw_job_params.get("run_id", "unknown"))

    def run(self) -> None:
        """Validate configuration and execute the job."""
        self.init_config()
        self.logger.info(
            f"[INIT_CONFIG] OK -> environment={self.job_config.environment} "
            f"type_origin={self.job_config.type_read} "
            f"datasets={self.job_config.dataset_ids}"
        )
        self.process()
        self.logger.info("[JOB] run completed successfully")

    def init_config(self) -> None:
        """Validate and build the JobConfig from raw_job_params."""
        self.job_config = JobConfig.from_dict(self.raw_job_params)

    @abstractmethod
    def process(self) -> None:
        """Run the job-specific logic (source_to_raw, raw_to_std...)."""
        raise NotImplementedError
