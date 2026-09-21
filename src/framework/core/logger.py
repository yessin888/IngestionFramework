"""Logging with run_id/dataset_id context."""
import logging
import sys


def get_logger(run_id: str, dataset_id: str = "-") -> logging.LoggerAdapter:
    """Build a logger adapter that prefixes every message with run/dataset context.

    Args:
        run_id: Identifier of the current job run.
        dataset_id: Identifier of the dataset being processed, if any.

    Returns:
        A LoggerAdapter bound to the shared "ingestion_framework" logger.
    """
    base_logger = logging.getLogger("ingestion_framework")
    if not base_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-7s | run_id=%(run_id)s | dataset_id=%(dataset_id)s | %(message)s"
        )
        handler.setFormatter(formatter)
        base_logger.addHandler(handler)
        base_logger.setLevel(logging.INFO)

    return logging.LoggerAdapter(base_logger, {"run_id": run_id, "dataset_id": dataset_id})
