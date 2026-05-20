from pyspark.sql.functions import substring, concat, lit, ceil, first, round as spark_round, sum as spark_sum, col, \
    count as spark_count, avg
from pyspark.sql import SparkSession

def task07_cross_country_aggregation(spark: SparkSession, fact_df, completeness_df, output_dir):
    f = fact_df.withColumn("typed_value_num", col("typed_value").cast("double")).withColumn("year", substring(col("period"),1,4)).withColumn("month", substring(col("period"),5,2).cast("int")).withColumn("quarter", concat(substring(col("period"),1,4), lit("-Q"), ceil(col("month")/lit(3))))
    volumes = f.groupBy("health_area","quarter").agg(spark_round(spark_sum("typed_value_num"),2).alias("total_service_volume"))
    volumes.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "service_volumes_by_healtharea_quarter"))
    comp_country = completeness_df.groupBy("country_name","period").agg(spark_round(avg("completeness_percent"),1).alias("avg_completeness_percent"))
    comp_country.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "completeness_by_country_period"))
    coverage = f.select("country_name","dataElement_uid").dropDuplicates().groupBy("dataElement_uid").pivot("country_name").agg(first(lit(1))).na.fill(0)
    coverage.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "dataelement_coverage_matrix"))
    low_counts = comp_country.filter(col("avg_completeness_percent")<80).groupBy("country_name").agg(spark_count("period").alias("low_periods"))
    low_countries = low_counts.filter(col("low_periods")>=3).select("country_name")
    low_countries.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "countries_below_80_for_3plus_periods"))
