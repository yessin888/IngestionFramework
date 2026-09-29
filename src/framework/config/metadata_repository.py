"""Access layer for the datasets and datasets_columns metadata tables."""
import os
from typing import List

from pyspark.sql import SparkSession

from framework.config.models import DatasetMetadata, DatasetColumnMetadata, DatasetQuality


class MetadataRepository:
    """Reads dataset configuration and column mappings from metadata CSVs."""

    def __init__(self, spark: SparkSession, metadata_base_path: str):
        """Initialize the repository.

        Args:
            spark: Active SparkSession.
            metadata_base_path: Directory containing datasets.csv and
                datasets_columns.csv.
        """
        self.spark = spark
        self.datasets_path = f"{metadata_base_path}/datasets.csv"
        self.datasets_columns_path = f"{metadata_base_path}/datasets_columns.csv"
        self.dataset_quality_path = f"{metadata_base_path}/dataset_quality.csv"

    def _read_metadata_csv(self, path: str):
        return (
            self.spark.read
            .option("header", "true")
            .option("sep", ";")
            .csv(path)
        )

    def get_datasets(self, dataset_ids: List[str]) -> List[DatasetMetadata]:
        """Return the datasets (with their columns) for the given ids.

        Args:
            dataset_ids: dataset_id values to look up.

        Returns:
            One DatasetMetadata per id, with `columns` already populated.

        Raises:
            ValueError: If any dataset_id has no matching row.
        """
        df = self._read_metadata_csv(self.datasets_path)
        df = df.filter(df.dataset_id.isin(dataset_ids))

        rows = df.collect()

        found_ids = {str(r["dataset_id"]) for r in rows}
        missing_ids = set(dataset_ids) - found_ids
        if missing_ids:
            raise ValueError(f"No metadata found for dataset_ids: {missing_ids}")

        datasets_metadata = [DatasetMetadata.from_row(r) for r in rows]
        for dataset in datasets_metadata:
            dataset.columns = self.get_columns(dataset.dataset_id)
            dataset.quality_rules = self.get_quality(dataset.dataset_id)

        return datasets_metadata

    def get_columns(self, dataset_id: str) -> List[DatasetColumnMetadata]:
        """Return the column mappings configured for a dataset, ordered by column_order.

        Args:
            dataset_id: dataset_id to look up.

        Returns:
            The dataset's columns sorted by column_order.

        Raises:
            ValueError: If no columns are configured for dataset_id.
        """
        df = self._read_metadata_csv(self.datasets_columns_path)
        df = df.filter(df.dataset_id == dataset_id)
        rows = df.collect()

        if not rows:
            raise ValueError(f"No columns configured in metadata for dataset_id={dataset_id}")

        columns = [DatasetColumnMetadata.from_row(r) for r in rows]
        columns.sort(key=lambda c: c.column_order)
        return columns

    def get_quality(self, dataset_id: str) -> List[DatasetQuality]:
        """Return the quality rules configured for a dataset.

        Args:
            dataset_id: dataset_id to look up.

        Returns:
            A DatasetQuality with no rules when dataset_quality.csv is
            missing or the dataset has no rules configured.
        """
        if not os.path.exists(self.dataset_quality_path):
            raise FileNotFoundError(f"Dataset quality metadata file not found at {self.dataset_quality_path}")

        df = self._read_metadata_csv(self.dataset_quality_path)
        rows = df.filter(df.dataset_id == dataset_id).collect()
        qualityRules = [DatasetQuality.from_row(r) for r in rows]
        return qualityRules
