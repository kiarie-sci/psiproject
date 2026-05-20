from pyspark.sql.functions import (
    split, col, when, broadcast, explode,
    element_at, regexp_extract
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def task03_resolve_hierarchy(spark, joined_df, ou_df):
    """
    Resolve org unit hierarchy by parsing the path column.
    Extract country, region, district, and facility UIDs and names.
    """

    # Parse the path: /country/region/district/facility
    # Path looks like: /vEexGiD193k/PMa2VCrupOd/rvN4VNz72Rn/kJq2dimyDsV
    df = joined_df.withColumn(
        "path_parts",
        split(col("orgUnit_path"), "/")
    )

    # Extract UIDs (skip empty first element)
    # element_at is 1-indexed
    df = df.withColumn("country_uid", element_at(col("path_parts"), 2)) \
        .withColumn("region_uid", element_at(col("path_parts"), 3)) \
        .withColumn("district_uid", element_at(col("path_parts"), 4)) \
        .withColumn("facility_uid", element_at(col("path_parts"), 5))

    # Build org unit lookup once
    ou_lookup = ou_df.select(
        explode(col("organisationUnits")).alias("ou")
    ).select(
        col("ou.id").alias("ou_id"),
        col("ou.name").alias("ou_name"),
        col("ou.level").alias("ou_level")
    ).dropDuplicates()

    # Broadcast the lookup for efficiency
    ou_lookup_bc = broadcast(ou_lookup)

    # Join for country
    df = df.join(
        ou_lookup_bc.select(
            col("ou_id").alias("country_uid_match"),
            col("ou_name").alias("country_name")
        ),
        col("country_uid") == col("country_uid_match"),
        "left"
    ).drop("country_uid_match")

    # Join for region
    df = df.join(
        ou_lookup_bc.select(
            col("ou_id").alias("region_uid_match"),
            col("ou_name").alias("region_name")
        ),
        col("region_uid") == col("region_uid_match"),
        "left"
    ).drop("region_uid_match")

    # Join for district
    df = df.join(
        ou_lookup_bc.select(
            col("ou_id").alias("district_uid_match"),
            col("ou_name").alias("district_name")
        ),
        col("district_uid") == col("district_uid_match"),
        "left"
    ).drop("district_uid_match")

    # Join for facility
    df = df.join(
        ou_lookup_bc.select(
            col("ou_id").alias("facility_uid_match"),
            col("ou_name").alias("facility_name"),
            col("ou_level").alias("facility_level")
        ),
        col("facility_uid") == col("facility_uid_match"),
        "left"
    ).drop("facility_uid_match")

    # Fallback: if joined names are null, use the names from left join in Task 02
    df = df.withColumn(
        "facility_name",
        when(col("facility_name").isNull(), col("orgUnit_name")).otherwise(col("facility_name"))
    ).withColumn(
        "facility_level",
        when(col("facility_level").isNull(), col("orgUnit_level")).otherwise(col("facility_level"))
    )

    # Clean up intermediate columns
    df = df.drop("path_parts", "country_uid", "region_uid", "district_uid", "facility_uid")

    return df
