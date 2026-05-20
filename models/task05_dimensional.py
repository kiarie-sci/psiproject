from pyspark.sql.functions import substring, concat, lit, ceil, explode, col
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql import SparkSession
from pathlib import Path


def task05_build_dimensions_and_fact(spark: SparkSession, df, meta_df, programs_df, output_dir: Path,
                                     incremental=False):
    """
    Build dimension and fact tables for the star schema.
    Dimensions: data_element, org_unit, period, program
    Fact: service_delivery (partitioned by health_area and year_month)
    """

    # dim_data_element
    de = meta_df.select(explode(col("dataElements")).alias("de")).select(
        col("de.id").alias("dataElement_uid"),
        col("de.name").alias("dataElement_name"),
        col("de.valueType").alias("valueType")
    ).dropDuplicates()
    de.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "dim_data_element"))

    # dim_org_unit (remove facility_uid, use orgUnit_uid)
    ou = df.select(
        "orgUnit_uid",
        "facility_name",
        "district_name",
        "region_name",
        "country_name",
        "facility_level"
    ).dropDuplicates()
    ou.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "dim_org_unit"))

    # dim_period
    dim_period = df.select(
        "period",
        "period_start",
        "period_end"
    ).dropDuplicates().withColumn(
        "year",
        substring(col("period"), 1, 4)
    ).withColumn(
        "month",
        substring(col("period"), 5, 2)
    ).withColumn(
        "year_month",
        concat(substring(col("period"), 1, 4), lit("-"), substring(col("period"), 5, 2))
    ).withColumn(
        "quarter",
        concat(substring(col("period"), 1, 4), lit("-Q"), ceil(col("month") / lit(3)))
    )
    dim_period.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "dim_period"))

    # dim_program
    prog_expl = programs_df.select(
        explode(col("programs")).alias("p")
    ).select(
        col("p.id").alias("program_uid"),
        col("p.healthArea").alias("health_area"),
        col("p.country").alias("country"),
        col("p.dataElements").alias("expected_dataElements")
    )
    prog_expl.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "dim_program"))

    # fact_service_delivery: join program mapping by dataElement and country
    # Rename the dataElement_uid to avoid column collision during join
    prog_map = prog_expl.select(
        col("program_uid"),
        col("health_area"),
        col("country"),
        explode(col("expected_dataElements")).alias("prog_de_uid")
    )

    fact = df.join(
        prog_map,
        (df["dataElement_uid"] == prog_map["prog_de_uid"]) &
        (df["country_name"] == prog_map["country"]),
        how="left"
    ).drop("prog_de_uid")  # Drop the temporary join column

    fact = fact.withColumn(
        "year_month",
        concat(substring(col("period"), 1, 4), lit("-"), substring(col("period"), 5, 2))
    )

    # Write fact partitioned by health_area and year_month
    fact.write.mode("overwrite").partitionBy("health_area", "year_month").parquet(
        str(output_dir / "fact_service_delivery")
    )

    return fact
