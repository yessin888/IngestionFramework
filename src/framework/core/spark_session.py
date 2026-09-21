"""Local SparkSession factory, with Delta Lake support enabled."""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark_session(app_name: str = "ingestion-framework") -> SparkSession:
    """Build a local SparkSession configured for Delta Lake.

    Args:
        app_name: Spark application name.

    Returns:
        A SparkSession with the Delta extensions and catalog configured.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
