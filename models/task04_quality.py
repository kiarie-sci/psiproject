from pyspark.sql.functions import (
    when, to_date, concat, substring, last_day, to_timestamp,
    datediff, trim, col, explode, collect_set, count as spark_count,
    round as spark_round, lit, coalesce
)
from pyspark.sql.types import IntegerType, DoubleType, BooleanType
from pyspark.sql import SparkSession
from pathlib import Path


def task04_quality_and_flags(spark, df, programs_df, output_dir: Path):
    """
    Cast raw_value to appropriate type based on valueType.
    Add quality flags: is_late_reported, is_explicit_zero, is_missing_value.
    Compute completeness_percent by facility-period.
    """

    # Create typed value columns separately to avoid type mismatch
    # Then coalesce them into a single column
    df = df.withColumn(
        "typed_value_bool",
        when(
            col("valueType") == "BOOLEAN",
            when(col("raw_value").isin("true", "True", "1"), True)
            .when(col("raw_value").isin("false", "False", "0"), False)
            .otherwise(None)
        )
    ).withColumn(
        "typed_value_int",
        when(
            col("valueType") == "INTEGER_ZERO_OR_POSITIVE",
            when(col("raw_value").rlike("^[0-9]+$"), col("raw_value").cast(IntegerType()))
            .otherwise(None)
        )
    ).withColumn(
        "typed_value_double",
        when(
            col("valueType").isin("NUMBER", "PERCENTAGE"),
            when(col("raw_value").rlike("^-?\\d+(\\.\\d+)?$"), col("raw_value").cast(DoubleType()))
            .otherwise(None)
        )
    )

    # Coalesce typed values into a single column (cast to double for consistency)
    df = df.withColumn(
        "typed_value",
        coalesce(
            col("typed_value_bool").cast(DoubleType()),
            col("typed_value_int").cast(DoubleType()),
            col("typed_value_double")
        )
    ).drop("typed_value_bool", "typed_value_int", "typed_value_double")

    # Parse period: YYYYMM format
    df = df.withColumn(
        "period_start",
        to_date(
            concat(
                substring(col("period"), 1, 4),
                lit("-"),
                substring(col("period"), 5, 2),
                lit("-01")
            )
        )
    ).withColumn(
        "period_end",
        last_day(col("period_start"))
    )

    # Parse lastUpdated timestamp and compute days since period end
    df = df.withColumn(
        "lastUpdated_ts",
        to_timestamp(col("lastUpdated"))
    ).withColumn(
        "lastUpdated_date",
        to_date(col("lastUpdated_ts"))
    )

    df = df.withColumn(
        "days_since_period_end",
        datediff(col("lastUpdated_date"), col("period_end"))
    )

    # Quality flags
    df = df.withColumn(
        "is_late_reported",
        when(col("days_since_period_end") > 60, True).otherwise(False)
    ).withColumn(
        "is_explicit_zero",
        when(col("raw_value") == "0", True).otherwise(False)
    ).withColumn(
        "is_missing_value",
        when(col("raw_value").isNull() | (trim(col("raw_value")) == ""), True).otherwise(False)
    )

    # Completeness: expected indicators per country
    prog_expl = programs_df.select(
        explode(col("programs")).alias("p")
    ).select(
        col("p.country").alias("country"),
        col("p.dataElements").alias("expected_dataElements")
    )

    expected_by_country = prog_expl.select(
        col("country"),
        explode(col("expected_dataElements")).alias("dataElement_uid")
    ).dropDuplicates().groupBy("country").agg(
        spark_count("dataElement_uid").alias("expected_indicators")
    )

    # Reported per facility-period
    reported = df.filter(
        col("typed_value").isNotNull()
    ).groupBy("orgUnit_uid", "facility_name", "country_name", "period").agg(
        spark_count("dataElement_uid").alias("reported_count"),
        collect_set("dataElement_uid").alias("reported_set")
    )

    # Join with expected and compute completeness
    completeness = reported.join(
        expected_by_country,
        reported["country_name"] == expected_by_country["country"],
        how="left"
    ).withColumn(
        "completeness_percent",
        when(
            col("expected_indicators").isNull(),
            None
        ).otherwise(
            spark_round((col("reported_count") / col("expected_indicators")) * 100, 1)
        )
    )

    # Write completeness to output
    completeness.coalesce(1).write.mode("overwrite").parquet(
        str(output_dir / "completeness_by_facility_period")
    )

    return df, completeness
