"""Domain models for the metadata tables (datasets, datasets_columns)."""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum



@dataclass
class DatasetColumnMetadata:
    """Represents one row of the datasets_columns metadata table."""

    dataset_id: str
    column_order: int
    source_name: Optional[str]
    source_type: Optional[str]
    dest_name: str
    dest_type: str
    is_sensitive: bool = False
    is_audit: bool = False
    is_business_key: bool = False

    @staticmethod
    def from_row(row) -> "DatasetColumnMetadata":
        """Build a DatasetColumnMetadata from a Spark Row.

        Args:
            row: Spark Row read from datasets_columns.csv.

        Returns:
            The parsed DatasetColumnMetadata instance.
        """
        return DatasetColumnMetadata(
            dataset_id=str(row["dataset_id"]),
            column_order=int(row["column_order"]),
            source_name=row["source_name"] or None,
            source_type=row["source_type"] or None,
            dest_name=row["dest_name"],
            dest_type=row["dest_type"],
            is_sensitive=str(row["is_sensitive"]).strip().upper() == "Y",
            is_audit=str(row["is_audit"]).strip().upper() == "Y",
            is_business_key=str(row["is_business_key"]).strip().upper() == "Y",
        )


@dataclass
class DatasetMetadata:
    """Represents one row of the datasets metadata table, with its columns."""

    dataset_id: str
    type_origin: str
    subtype_origin: str
    tablename: str
    type_read: str
    type_file: Optional[str]
    source_path: str
    raw_path: str
    std_path: str
    type_load: str
    incremental_field: Optional[str]
    incremental_mask: Optional[str]
    partition_field: Optional[str]
    columns: List[DatasetColumnMetadata] = field(default_factory=list)

    @staticmethod
    def from_row(row) -> "DatasetMetadata":
        """Build a DatasetMetadata from a Spark Row.

        Args:
            row: Spark Row read from datasets.csv.

        Returns:
            The parsed DatasetMetadata instance (columns not populated).
        """
        def _empty_to_none(v):
            return v if v not in (None, "",) else None

        return DatasetMetadata(
            dataset_id=str(row["dataset_id"]),
            type_origin=row["type_origin"],
            subtype_origin=row["subtype_origin"],
            tablename=row["tablename"],
            type_read=row["type_read"],
            type_file=_empty_to_none(row["type_file"]),
            source_path=row["source_path"],
            raw_path=row["raw_path"],
            std_path=row["std_path"],
            type_load=row["type_load"],
            incremental_field=_empty_to_none(row["incremental_field"]),
            incremental_mask=_empty_to_none(row["incremental_mask"]),
            partition_field=_empty_to_none(row["partition_field"]),
        )

    def non_audit_columns(self) -> List[DatasetColumnMetadata]:
        """Return all columns where is_audit is False."""
        return [c for c in self.columns if not c.is_audit]

    def audit_columns(self) -> List[DatasetColumnMetadata]:
        """Return all columns where is_audit is True."""
        return [c for c in self.columns if c.is_audit]

    def sensitive_columns(self) -> List[DatasetColumnMetadata]:
        """Return all columns where is_sensitive is True."""
        return [c for c in self.columns if c.is_sensitive]

    def business_key_columns(self) -> List[DatasetColumnMetadata]:
        """Return all columns where is_business_key is True."""
        return [c for c in self.columns if c.is_business_key]

    def dest_name_for(self, source_name: Optional[str]) -> Optional[str]:
        """Translate a column name from source_name space to dest_name space.

        Args:
            source_name: Column name as it appears in the source/raw schema.

        Returns:
            The mapped dest_name, source_name unchanged if no mapping is
            found, or None if source_name is None.
        """
        if not source_name:
            return None
        for c in self.columns:
            if c.source_name == source_name:
                return c.dest_name
        return source_name


class Status(Enum):
    SUCCEEDED = "SUCCEEDED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"

@dataclass
class ControlData:
    dataset_id: str
    run_id: str
    job_name: str
    type_origin: str
    subtype_origin: str
    tablename: str
    type_read: str
    start_time: datetime
    end_time: datetime
    status: Status
    readed_rows: int
    written_rows: int
    error_rows: int

    # Campos opcionales - dataclass requiere valores por defecto al final 
    status_message: Optional[str] = None
    old_watermark: Optional[str] = None
    new_watermark: Optional[str] = None

    @staticmethod
    def from_row(row: dict) -> "ControlData":
        return ControlData(
            dataset_id=row["dataset_id"],
            run_id=row["run_id"],
            job_name=row["job_name"],
            type_origin=row["type_origin"],
            subtype_origin=row["subtype_origin"],
            tablename=row["tablename"],
            type_read=row["type_read"],
            start_time=row["start_time"],
            end_time=row["end_time"],
            status=Status(row["status"]),
            status_message=row.get("status_message"),
            readed_rows=row["readed_rows"],
            written_rows=row["written_rows"],
            error_rows=row["error_rows"],
            old_watermark=row.get("old_watermark"),
            new_watermark=row.get("new_watermark"),
        )

    def to_spark_row(self) -> dict:
        """Serialize the control record into Spark-compatible primitive values."""
        return {
            "dataset_id": self.dataset_id,
            "run_id": self.run_id,
            "job_name": self.job_name,
            "type_origin": self.type_origin,
            "subtype_origin": self.subtype_origin,
            "tablename": self.tablename,
            "type_read": self.type_read,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status.value,
            "readed_rows": self.readed_rows,
            "written_rows": self.written_rows,
            "error_rows": self.error_rows,
            "status_message": self.status_message,
            "old_watermark": self.old_watermark,
            "new_watermark": self.new_watermark,
        }
