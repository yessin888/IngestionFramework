"""Factory that resolves which reader to use for a given type_read."""
from pyspark.sql import SparkSession

from framework.readers.base_reader import BaseReader
from framework.readers.file_reader import FileReader


class ReaderFactory:
    """Maps type_read to a reader implementation."""

    _REGISTRY = {
        "STORAGE_READ": lambda spark: FileReader(spark)
        # "JDBC": lambda spark: JdbcReader(spark, url, props),
        # "API": lambda spark: ApiReader(spark, api_config),
    }

    @staticmethod
    def get_reader(spark: SparkSession, type_read: str) -> BaseReader:
        """Instantiate the reader registered for type_read.

        Args:
            spark: Active SparkSession.
            type_read: Read strategy key (e.g. STORAGE_READ, JDBC, API).

        Returns:
            The reader instance for type_read.

        Raises:
            ValueError: If no reader is registered for type_read.
        """
        key = type_read.upper()
        if key not in ReaderFactory._REGISTRY:
            raise ValueError(
                f"No reader registered for type_read={type_read}. "
                f"Available readers: {list(ReaderFactory._REGISTRY.keys())}"
            )
        return ReaderFactory._REGISTRY[key](spark)
