"""Common contract implemented by every source reader."""
from abc import ABC, abstractmethod
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from framework.config.job_config import JobConfig
from framework.config.models import DatasetMetadata


class BaseReader(ABC):
    """Base class for readers, one per (type_origin, subtype_origin)."""

    def __init__(self, spark: SparkSession):
        """Initialize the reader.

        Args:
            spark: Active SparkSession.
        """
        self.spark = spark

    @abstractmethod
    def read(self, dataset: DatasetMetadata, job_config: JobConfig, watermark: Optional[str] = None) -> DataFrame:
        """Read raw data from the source, without column selection, hashing or audit columns.

        Args:
            dataset: Metadata of the dataset to read.
            job_config: Current job configuration.
            watermark: Last value already present in this layer's destination
                (None if the destination does not exist yet, meaning a full
                initial load). Resolved by the caller via
                incremental.get_current_watermark.

        Returns:
            The raw DataFrame read from the source.
        """
        raise NotImplementedError
