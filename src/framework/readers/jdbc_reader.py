"""Extension point for real JDBC sources (Oracle, SAP HANA, SQL Server...)."""
from typing import Optional

from pyspark.sql import DataFrame

from framework.config.job_config import JobConfig
from framework.config.models import DatasetMetadata
from framework.readers.base_reader import BaseReader


class JdbcReader(BaseReader):
    """Skeleton for a real JDBC source reader."""

    def __init__(self, spark, jdbc_url: str, connection_properties: dict):
        """Initialize the reader.

        Args:
            spark: Active SparkSession.
            jdbc_url: JDBC connection URL.
            connection_properties: JDBC connection properties (user, password, driver...).
        """
        super().__init__(spark)
        self.jdbc_url = jdbc_url
        self.connection_properties = connection_properties

    def read(self, dataset: DatasetMetadata, job_config: JobConfig, watermark: Optional[str] = None) -> DataFrame:
        """Not implemented; documents the intended JDBC read strategy.

        Args:
            dataset: Metadata of the dataset to read.
            job_config: Current job configuration.
            watermark: Last watermark for this layer, or None for a full load.

        Raises:
            NotImplementedError: Always. Real implementation should use
                spark.read.jdbc(url, table=dataset.tablename,
                properties=connection_properties), pushing the incremental
                filter down via "predicates" or a partitioning column.
        """
        raise NotImplementedError(
            "JdbcReader is an example skeleton, not implemented."
        )
