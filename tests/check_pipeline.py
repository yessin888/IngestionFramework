"""End-to-end verification of the source_raw -> raw_std pipeline.

Usage: python tests/check_pipeline.py

Each scenario (incremental, batch) builds its own isolated workspace
under tests/_workspace and its own metadata from a small Python
definition (DatasetDef/ColumnDef) instead of hand-written CSV text, so
changing a destination name, a cast, a path or a flag is a one-line
edit. Never touches the project's real data or metadata.
"""
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta.tables import DeltaTable

from framework.config.metadata_repository import MetadataRepository
from framework.core.spark_session import get_spark_session
from framework.jobs.raw_to_std_job import RawToStdJob
from framework.jobs.source_to_raw_job import SourceToRawJob

WORKSPACE = PROJECT_ROOT / "tests" / "_workspace"

DATASETS_HEADER = (
    "dataset_id;type_origin;subtype_origin;tablename;type_read;type_file;source_path;raw_path;std_path;"
    "type_load;incremental_field;incremental_mask;partition_field"
)
COLUMNS_HEADER = (
    "dataset_id;column_order;source_name;source_type;dest_name;dest_type;is_sensitive;is_audit;is_business_key"
)

_passed = 0
_failed = 0


@dataclass
class ColumnDef:
    """Configurable definition of one datasets_columns row."""

    name: str
    source_type: str = "string"
    dest_name: Optional[str] = None
    dest_type: Optional[str] = None
    sensitive: bool = False
    audit: bool = False
    business_key: bool = False

    def __post_init__(self):
        if self.dest_name is None:
            self.dest_name = self.name
        if self.dest_type is None:
            self.dest_type = self.source_type


@dataclass
class DatasetDef:
    """Configurable definition of one datasets row and its columns."""

    dataset_id: str
    tablename: str
    type_load: str
    landing_path: Path
    raw_path: Path
    std_path: Path
    columns: List[ColumnDef]
    type_origin: str = "TEST"
    type_read: str = "STORAGE_READ"
    type_file: str = "CSV"
    incremental_field: str = ""
    incremental_mask: str = ""
    partition_field: str = ""


def render_datasets_csv(datasets: List[DatasetDef]) -> str:
    """Render a list of DatasetDef into datasets.csv content.

    Args:
        datasets: Dataset definitions to render.

    Returns:
        The full datasets.csv text, including header.
    """
    lines = [DATASETS_HEADER]
    for d in datasets:
        lines.append(";".join([
            d.dataset_id, d.type_origin, "", d.tablename, d.type_read, d.type_file,
            d.landing_path.as_posix(), d.raw_path.as_posix(), d.std_path.as_posix(),
            d.type_load, d.incremental_field, d.incremental_mask, d.partition_field,
        ]))
    return "\n".join(lines) + "\n"


def render_columns_csv(datasets: List[DatasetDef]) -> str:
    """Render a list of DatasetDef into datasets_columns.csv content.

    Args:
        datasets: Dataset definitions whose columns should be rendered.

    Returns:
        The full datasets_columns.csv text, including header.
    """
    lines = [COLUMNS_HEADER]
    for d in datasets:
        for order, c in enumerate(d.columns, start=1):
            lines.append(";".join([
                d.dataset_id, str(order), c.name, c.source_type, c.dest_name, c.dest_type,
                "Y" if c.sensitive else "N", "Y" if c.audit else "N", "Y" if c.business_key else "N",
            ]))
    return "\n".join(lines) + "\n"


def write_metadata(config_dir: Path, datasets: List[DatasetDef]) -> None:
    """Write datasets.csv and datasets_columns.csv for the given definitions.

    Args:
        config_dir: Directory to write the metadata files into.
        datasets: Dataset definitions to persist.
    """
    (config_dir / "datasets.csv").write_text(render_datasets_csv(datasets), encoding="utf-8")
    (config_dir / "datasets_columns.csv").write_text(render_columns_csv(datasets), encoding="utf-8")


def write_landing_csv(path: Path, columns: List[str], rows) -> None:
    """Write a landing CSV file.

    Args:
        path: Destination file path.
        columns: Column names, used as the CSV header.
        rows: Iterable of tuples matching columns.
    """
    lines = [",".join(columns)] + [",".join(r) for r in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def run_source_raw(spark, repo, run_id: str, dataset_ids: List[str]) -> None:
    """Run SourceToRawJob for the given dataset ids.

    Args:
        spark: Active SparkSession.
        repo: MetadataRepository pointing at the test workspace.
        run_id: Run identifier.
        dataset_ids: Dataset ids to process.
    """
    SourceToRawJob(spark, repo, {
        "job_timestamp": "2026-01-01T00:00:00",
        "environment": "dev",
        "type_read": "STORAGE_READ",
        "run_id": run_id,
        "dataset_ids": dataset_ids,
    }).run()


def run_raw_std(spark, repo, run_id: str, dataset_ids: List[str]) -> None:
    """Run RawToStdJob for the given dataset ids.

    Args:
        spark: Active SparkSession.
        repo: MetadataRepository pointing at the test workspace.
        run_id: Run identifier.
        dataset_ids: Dataset ids to process.
    """
    RawToStdJob(spark, repo, {
        "job_timestamp": "2026-01-01T00:00:00",
        "environment": "dev",
        "type_read": "STORAGE_READ",
        "run_id": run_id,
        "dataset_ids": dataset_ids,
    }).run()


def check(description: str, condition: bool) -> None:
    """Record and print the result of a single check.

    Args:
        description: Human-readable description of what was checked.
        condition: Result of the check.
    """
    global _passed, _failed
    if condition:
        print(f"  OK   {description}")
        _passed += 1
    else:
        print(f"  FAIL {description}")
        _failed += 1


def check_incremental_scenario(spark) -> None:
    """Verify the INCREMENTAL path: watermark derivation, append, and MERGE.

    Args:
        spark: Active SparkSession.
    """
    print("=== INCREMENTAL scenario ===")
    root = WORKSPACE / "incremental"
    if root.exists():
        shutil.rmtree(root)
    landing_dir = root / "landing"
    config_dir = root / "config"
    landing_dir.mkdir(parents=True)
    config_dir.mkdir(parents=True)

    dataset = DatasetDef(
        dataset_id="1",
        tablename="events",
        type_load="INCREMENTAL",
        landing_path=landing_dir,
        raw_path=root / "raw" / "events",
        std_path=root / "std" / "events",
        incremental_field="updated_at",
        incremental_mask="yyyy-MM-dd",
        partition_field="updated_at",
        columns=[
            ColumnDef("id", business_key=True),
            ColumnDef("name"),
            ColumnDef("email", sensitive=True),
            ColumnDef("updated_at"),
        ],
    )
    write_metadata(config_dir, [dataset])
    repo = MetadataRepository(spark, metadata_base_path=str(config_dir))
    landing_csv = landing_dir / "events.csv"
    columns = ["id", "name", "email", "updated_at"]

    print("1) Initial load (2 rows)")
    write_landing_csv(landing_csv, columns, [
        ("e1", "Login", "a@x.com", "2026-01-01"),
        ("e2", "Logout", "b@x.com", "2026-01-02"),
    ])
    run_source_raw(spark, repo, "check_1", ["1"])
    run_raw_std(spark, repo, "check_1", ["1"])

    raw_df = spark.read.format("delta").load(str(dataset.raw_path))
    std_df = spark.read.format("delta").load(str(dataset.std_path))

    check("raw is a Delta table", DeltaTable.isDeltaTable(spark, str(dataset.raw_path)))
    check("raw has the 2 initial rows", raw_df.count() == 2)
    check("raw is partitioned by updated_at", (dataset.raw_path / "updated_at=2026-01-01").exists())
    check("std has the 2 initial rows", std_df.count() == 2)
    check(
        "std has the email hashed (not in plain text)",
        all(r["email"] not in ("a@x.com", "b@x.com") for r in std_df.collect()),
    )

    print("\n2) Incremental: new row + existing row that changes partition")
    write_landing_csv(landing_csv, columns, [
        ("e1", "Login-Updated", "a@x.com", "2026-01-05"),
        ("e2", "Logout", "b@x.com", "2026-01-02"),
        ("e3", "Purchase", "c@x.com", "2026-01-06"),
    ])
    run_source_raw(spark, repo, "check_2", ["1"])
    run_raw_std(spark, repo, "check_2", ["1"])

    raw_df = spark.read.format("delta").load(str(dataset.raw_path))
    std_df = spark.read.format("delta").load(str(dataset.std_path))

    check("raw only appended the 2 new/changed rows (e2 not reprocessed)", raw_df.count() == 4)
    check("raw was also partitioned by the new date", (dataset.raw_path / "updated_at=2026-01-06").exists())
    check("std still has 3 rows (MERGE did not duplicate e1)", std_df.count() == 3)
    e1_rows = std_df.filter("id = 'e1'").collect()
    check(
        "std updated e1 in place (single row, with the new value)",
        len(e1_rows) == 1 and e1_rows[0]["name"] == "Login-Updated",
    )

    print("\n3) Rerun with no new data (idempotency)")
    run_source_raw(spark, repo, "check_3", ["1"])
    run_raw_std(spark, repo, "check_3", ["1"])

    raw_df = spark.read.format("delta").load(str(dataset.raw_path))
    std_df = spark.read.format("delta").load(str(dataset.std_path))
    check("raw did not grow (nothing new to read)", raw_df.count() == 4)
    check("std did not grow", std_df.count() == 3)


def check_batch_scenario(spark) -> None:
    """Verify the FULL path: bootstrap and overwrite-based rerun.

    Args:
        spark: Active SparkSession.
    """
    print("\n=== BATCH (FULL) scenario ===")
    root = WORKSPACE / "batch"
    if root.exists():
        shutil.rmtree(root)
    landing_dir = root / "landing"
    config_dir = root / "config"
    landing_dir.mkdir(parents=True)
    config_dir.mkdir(parents=True)

    dataset = DatasetDef(
        dataset_id="2",
        tablename="customers",
        type_load="FULL",
        landing_path=landing_dir,
        raw_path=root / "raw" / "customers",
        std_path=root / "std" / "customers",
        columns=[
            ColumnDef("id", dest_name="customer_id", business_key=True),
            ColumnDef("name"),
            ColumnDef("email", sensitive=True),
        ],
    )
    write_metadata(config_dir, [dataset])
    repo = MetadataRepository(spark, metadata_base_path=str(config_dir))
    landing_csv = landing_dir / "customers.csv"
    columns = ["id", "name", "email"]

    print("1) Bootstrap (2 rows)")
    write_landing_csv(landing_csv, columns, [
        ("c1", "Ana", "ana@x.com"),
        ("c2", "Luis", "luis@x.com"),
    ])
    run_source_raw(spark, repo, "check_4a", ["2"])
    run_raw_std(spark, repo, "check_4a", ["2"])

    raw_df = spark.read.format("delta").load(str(dataset.raw_path))
    std_df = spark.read.format("delta").load(str(dataset.std_path))
    check("raw has the 2 initial rows", raw_df.count() == 2)
    check("std has the 2 initial rows", std_df.count() == 2)
    check("std renamed 'id' to 'customer_id'", "customer_id" in std_df.columns)

    print("\n2) Rerun: one row changes, one row is new")
    write_landing_csv(landing_csv, columns, [
        ("c1", "Ana Updated", "ana@x.com"),
        ("c2", "Luis", "luis@x.com"),
        ("c3", "Marta", "marta@x.com"),
    ])
    run_source_raw(spark, repo, "check_4b", ["2"])
    run_raw_std(spark, repo, "check_4b", ["2"])

    raw_df = spark.read.format("delta").load(str(dataset.raw_path))
    std_df = spark.read.format("delta").load(str(dataset.std_path))
    check("raw was overwritten to 3 rows (old ones not accumulated)", raw_df.count() == 3)
    check("std has 3 rows, c1 not duplicated", std_df.count() == 3)
    c1_rows = std_df.filter("customer_id = 'c1'").collect()
    check("std c1 ended up with the new name", len(c1_rows) == 1 and c1_rows[0]["name"] == "Ana Updated")


def main() -> None:
    """Run every scenario and print the final summary."""
    spark = get_spark_session(app_name="pipeline-check")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        check_incremental_scenario(spark)
        check_batch_scenario(spark)
    finally:
        spark.stop()

    print(f"\n{_passed} OK, {_failed} FAIL")
    if _failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
