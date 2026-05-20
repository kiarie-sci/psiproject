from pyspark.sql import Window
from pyspark.sql.functions import lag, avg, rank, col, sum as spark_sum, round as spark_round, count as spark_count, \
    when
from pyspark.sql import SparkSession

def task06_program_analytics(spark: SparkSession, fact_df, output_dir):
    f = fact_df.withColumn("typed_value_num", col("typed_value").cast("double"))
    # MoM per indicator per district
    w = Window.partitionBy("dataElement_uid","district_name").orderBy("period")
    mom = f.withColumn("prev", lag("typed_value_num").over(w)).withColumn("mom_pct", when(col("prev").isNull(), None).otherwise((col("typed_value_num")-col("prev"))/col("prev")*100))
    mom.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "mom_pct_change_by_indicator_district"))
    # rolling 3-month avg
    w2 = Window.partitionBy("orgUnit_uid","dataElement_uid").orderBy("period").rowsBetween(-2,0)
    roll = f.withColumn("rolling_3m_avg", avg("typed_value_num").over(w2))
    roll.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "rolling_3m_avg"))
    # country-level reporting rate
    expected = f.filter(col("facility_level")==4).select("country_name","orgUnit_uid").dropDuplicates().groupBy("country_name").agg(spark_count("orgUnit_uid").alias("expected_facilities"))
    reported = f.select("country_name","period","orgUnit_uid").dropDuplicates().groupBy("country_name","period").agg(spark_count("orgUnit_uid").alias("reported_facilities"))
    report_rate = reported.join(expected, on="country_name", how="left").withColumn("reporting_rate_percent", when(col("expected_facilities").isNull(), None).otherwise(spark_round((col("reported_facilities")/col("expected_facilities"))*100,1)))
    report_rate.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "country_reporting_rate"))
    # top 5 underreporting per health area
    zero_flags = f.withColumn("is_zero", when(col("typed_value_num")==0,1).otherwise(0))
    zero_counts = zero_flags.groupBy("health_area","orgUnit_uid","facility_name").agg(spark_sum("is_zero").alias("zero_periods"))
    w_rank = Window.partitionBy("health_area").orderBy(col("zero_periods").desc())
    under_top5 = zero_counts.withColumn("r", rank().over(w_rank)).filter(col("r")<=5)
    under_top5.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "top5_underreporting_facilities_per_health_area"))
