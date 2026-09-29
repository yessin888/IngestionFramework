
from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count

from framework.config.models import DatasetQuality, qualityRule


def check_quality(df: DataFrame, quality_checks: list[DatasetQuality], logger):

    rules = _compile_quality_rules(quality_checks)
    pk_cols = _extract_primary_key_columns(quality_checks)

    flags = [r.expression.alias(f"{r.name}_flag") for r in rules]

    if pk_cols:
        pk_dup = (count("*").over(Window.partitionBy(*pk_cols)) < 1)
        pk_null = reduce(or_, (col(c).isNotNull() for c in pk_cols))
        pk_flag = (pk_dup | pk_null).alias("pk_flag")

    flagged_df = df.select("*", *flags, pk_flag) if pk_cols else df.select("*", *flags)

    logger.info("Flagged DataFrame:")
    flagged_df.show()
	

    return flagged_df


def _extract_primary_key_columns(quality_checks: list[DatasetQuality]) -> list:
    pk_cols = [qc.pk_column for qc in quality_checks if qc.pk_column]
    return pk_cols if pk_cols else None



def _compile_quality_rules(quality_checks: DatasetQuality) -> list:
    """Serialize quality rules using numbered rule types."""
    result = []
    for dataset_quality in quality_checks:
        for rule_type, rules in (
            ("ck_rule", dataset_quality.ck_rules),
            ("regex_rule", dataset_quality.regex_rules),
            ("null_rule", dataset_quality.null_rules),
        ):
            for i, rule in enumerate(rules, start=1):
                result.append(
                    qualityRule(name=f"{rule_type}_{i}", expression=F.expr(rule))
                )

    return result