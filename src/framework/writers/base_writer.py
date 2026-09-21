"""Common contract implemented by every layer writer."""
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from framework.config.models import DatasetMetadata


class BaseWriter(ABC):
    """Base class for writers, one per medallion layer (raw, std...)."""

    @abstractmethod
    def write(self, df: DataFrame, dataset: DatasetMetadata, logger) -> None:
        """Persist df into the layer's target path.

        Args:
            df: DataFrame to write.
            dataset: Metadata of the dataset being written.
            logger: Logger used to report the write.
        """
        raise NotImplementedError
