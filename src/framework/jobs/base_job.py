"""Template Method skeleton (init_config -> process) shared by every job."""
from abc import ABC, abstractmethod

from framework.config.job_config import JobConfig
from framework.config.metadata_repository import MetadataRepository
from framework.config.models import ControlData
from framework.core.logger import get_logger
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

CONTROL_TABLE_PATH = "data/log/control_table"
CONTROL_DATA_SCHEMA = StructType([
    StructField("dataset_id", StringType(), nullable=False),
    StructField("run_id", StringType(), nullable=False),
    StructField("job_name", StringType(), nullable=False),
    StructField("job_timestamp", TimestampType(), nullable=False),
    StructField("type_origin", StringType(), nullable=True),
    StructField("subtype_origin", StringType(), nullable=True),
    StructField("tablename", StringType(), nullable=False),
    StructField("type_read", StringType(), nullable=False),
    StructField("start_time", TimestampType(), nullable=False),
    StructField("end_time", TimestampType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("readed_rows", LongType(), nullable=False),
    StructField("written_rows", LongType(), nullable=False),
    StructField("error_rows", LongType(), nullable=False),
    StructField("status_message", StringType(), nullable=True),
    StructField("old_watermark", StringType(), nullable=True),
    StructField("new_watermark", StringType(), nullable=True),
])

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
        self.control_data: list[ControlData]

    def run(self) -> None:
        """Validate configuration and execute the job."""
        self.init_config()
        self.control_data = [] # Initialize the list of ControlData objects for the current job run.
        self.logger.info(
            f"[INIT_CONFIG] OK -> environment={self.job_config.environment} "
            f"type_origin={self.job_config.type_read} "
            f"datasets={self.job_config.dataset_ids}"
        )
        self.process()
        self.store_control_data()
        self.logger.info("[JOB] run completed successfully")


    def init_config(self) -> None:
        """Validate and build the JobConfig from raw_job_params."""
        self.job_config = JobConfig.from_dict(self.raw_job_params)

    def store_control_data(self) -> None:
        """Persist accumulated control records in the Delta control table."""
        if not self.control_data:
            self.logger.info("[CONTROL_DATA] no records to persist")
            return

        rows = [control_record.to_spark_row() for control_record in self.control_data]

        df = self.spark.createDataFrame(
            data=rows,
            schema=CONTROL_DATA_SCHEMA,
        )

        df.write.format("delta").mode("append").save(CONTROL_TABLE_PATH)

        self.logger.info(
            f"[CONTROL_DATA] persisted {len(self.control_data)} records "
            f"to {CONTROL_TABLE_PATH}"
        )

    @abstractmethod
    def process(self) -> None:
        """Run the job-specific logic (source_to_raw, raw_to_std...).

        Args:
            control_data is now an instance attribute: self.control_data.
        """
        raise NotImplementedError
