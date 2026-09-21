"""Parsing and validation of ingestion job parameters."""
from dataclasses import dataclass
from datetime import datetime
from typing import List


ALLOWED_ENVIRONMENTS = {"dev", "pre", "pro"}
ALLOWED_TYPE_READ = {"STORAGE_READ", "JDBC", "API"}


class InvalidJobConfigError(Exception):
    """Raised when the job input parameters fail validation."""
    pass


@dataclass
class JobConfig:
    """Validated parameters a job is launched with."""

    job_timestamp: datetime
    environment: str
    type_read: str
    run_id: str
    dataset_ids: List[str]

    @staticmethod
    def from_dict(raw: dict) -> "JobConfig":
        """Build and validate a JobConfig from a raw parameter dict.

        Args:
            raw: Raw parameters, e.g. the result of parsing argparse or
                dbutils.widgets.

        Returns:
            The validated JobConfig instance.

        Raises:
            InvalidJobConfigError: If any parameter is missing or invalid.
                All validation errors are collected before raising.
        """
        job_timestamp_str = raw.get("job_timestamp")
        environment = raw.get("environment")
        type_read = raw.get("type_read")
        run_id = raw.get("run_id")
        dataset_ids_raw = raw.get("dataset_ids")

        errors = []

        job_timestamp = None
        if not job_timestamp_str:
            errors.append("job_timestamp is required (format: yyyy-MM-ddTHH:mm:ss)")
        else:
            try:
                job_timestamp = datetime.fromisoformat(job_timestamp_str)
            except ValueError:
                errors.append(
                    f"job_timestamp '{job_timestamp_str}' is not a valid ISO format "
                    f"(yyyy-MM-ddTHH:mm:ss)"
                )

        if not environment:
            errors.append("environment is required")
        elif environment not in ALLOWED_ENVIRONMENTS:
            errors.append(f"environment '{environment}' is not valid. Allowed: {ALLOWED_ENVIRONMENTS}")

        if not type_read:
            errors.append("type_read is required")
        elif type_read not in ALLOWED_TYPE_READ:
            errors.append(f"type_read '{type_read}' is not valid. Allowed: {ALLOWED_TYPE_READ}")

        if not run_id:
            errors.append("run_id is required")

        dataset_ids = []
        if not dataset_ids_raw:
            errors.append("dataset_ids is required and cannot be empty")
        else:
            if isinstance(dataset_ids_raw, str):
                dataset_ids = [d.strip() for d in dataset_ids_raw.split(",") if d.strip()]
            else:
                dataset_ids = list(dataset_ids_raw)
            if not dataset_ids:
                errors.append("dataset_ids cannot resolve to an empty list")

        if errors:
            raise InvalidJobConfigError("Invalid job parameters:\n - " + "\n - ".join(errors))

        return JobConfig(
            job_timestamp=job_timestamp,
            environment=environment,
            type_read=type_read,
            run_id=run_id,
            dataset_ids=dataset_ids,
        )
