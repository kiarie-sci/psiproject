from pyspark.sql.functions import split, array_remove, element_at, col, when, broadcast, explode, lit
from pyspark.sql import SparkSession

def task03_resolve_hierarchy(spark, joined_df, ou_df):
    df = joined_df.withColumn("path_uids", array_remove(split(col("orgUnit_path"), "/"), lit("")))
    df = df.withColumn("country_uid", element_at(col("path_uids"), 1)).withColumn("region_uid", element_at(col("path_uids"), 2)).withColumn("district_uid", element_at(col("path_uids"), 3)).withColumn("facility_uid", element_at(col("path_uids"), -1))
    lu = broadcast(ou_df.select(explode(col("organisationUnits")).alias("ou"))).select(col("ou.id").alias("lu_uid"), col("ou.name").alias("lu_name"), col("ou.level").alias("lu_level"))
    df = df.join(lu.select(col("lu_uid").alias("country_uid_lu"), col("lu_name").alias("country_name")), df["country_uid"] == col("country_uid_lu"), "left") \
           .join(lu.select(col("lu_uid").alias("region_uid_lu"), col("lu_name").alias("region_name")), df["region_uid"] == col("region_uid_lu"), "left") \
           .join(lu.select(col("lu_uid").alias("district_uid_lu"), col("lu_name").alias("district_name")), df["district_uid"] == col("district_uid_lu"), "left") \
           .join(lu.select(col("lu_uid").alias("facility_uid_lu"), col("lu_name").alias("facility_name"), col("lu_level").alias("facility_level")), df["facility_uid"] == col("facility_uid_lu"), "left")
    df = df.withColumn("facility_name", when(col("facility_name").isNull(), col("orgUnit_name")).otherwise(col("facility_name"))).withColumn("facility_level", when(col("facility_level").isNull(), col("orgUnit_level")).otherwise(col("facility_level")))
    df = df.drop("country_uid_lu", "region_uid_lu", "district_uid_lu", "facility_uid_lu")
    return df
