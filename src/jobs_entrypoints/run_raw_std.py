"""Entrypoint for the "raw -> std" job.

Same CLI parameter contract, SparkSession and MetadataRepository as
run_source_raw.py; only the orchestrated job differs.

Example usage (from the project root, with src/ on the PYTHONPATH):

    python src/jobs_entrypoints/run_raw_std.py \\
        --job_timestamp 2026-07-10T08:00:00 \\
        --environment dev \\
        --type_read STORAGE_READ \\
        --run_id run_20260710_003 \\
        --dataset_ids 1,2
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from framework.config.job_config import InvalidJobConfigError
from framework.config.metadata_repository import MetadataRepository
from framework.core.spark_session import get_spark_session
from framework.jobs.raw_to_std_job import RawToStdJob


def parse_args() -> dict:
    """Parse CLI arguments into a raw job parameters dict.

    Returns:
        The parsed arguments as a dict.
    """
    parser = argparse.ArgumentParser(description="raw_to_std job of the ingestion-framework")
    parser.add_argument("--job_timestamp", required=True, help="ISO format: yyyy-MM-ddTHH:mm:ss")
    parser.add_argument("--environment", required=True, choices=["dev", "pre", "pro"])
    parser.add_argument("--type_read", required=True, choices=["STORAGE_READ", "JDBC", "API"])
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--dataset_ids", required=True, help="Comma-separated list, e.g. 1,2,3")
    parser.add_argument(
        "--metadata_path",
        required=False,
        default=os.path.join(os.path.dirname(__file__), "..", "..", "config", "metadata"),
    )
    args = parser.parse_args()
    return vars(args)


def main() -> None:
    """Parse arguments, build the job and run it."""
    raw_params = parse_args()
    metadata_path = raw_params.pop("metadata_path")

    spark = get_spark_session(app_name=f"raw_to_std-{raw_params.get('run_id')}")

    try:
        metadata_repository = MetadataRepository(spark, metadata_base_path=metadata_path)
        job = RawToStdJob(spark, metadata_repository, raw_params)
        job.run()
    except InvalidJobConfigError as e:
        print(f"Invalid job configuration: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
