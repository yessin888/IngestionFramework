"""Quick viewer for the Delta tables written by the pipeline.

Usage: python tests/show_delta.py
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from framework.core.spark_session import get_spark_session


def show_delta_table(spark, path: str, label: str = "") -> None:
    """Read a Delta table and print its schema, rows and row count.

    Args:
        spark: Active SparkSession.
        path: Path of the Delta table.
        label: Optional label printed above the table, defaults to path.
    """
    print(f"\n=== {label or path} ===")
    if not Path(path).exists():
        print(f"  path does not exist: {path}")
        return

    df = spark.read.format("delta").load(path)
    df.printSchema()
    df.show(truncate=False)
    print(f"  rows: {df.count()}")


if __name__ == "__main__":
    spark = get_spark_session(app_name="show-delta")
    spark.sparkContext.setLogLevel("ERROR")

    try:
        show_delta_table(spark, "data/raw/sales", "raw: sales")
        show_delta_table(spark, "data/std/sales", "std: sales")
        show_delta_table(spark, "data/raw/customers", "raw: customers")
        show_delta_table(spark, "data/std/customers", "std: customers")
    finally:
        spark.stop()
