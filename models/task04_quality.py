from pyspark.sql.functions import when, to_date, concat, substring, last_day, to_timestamp, datediff, trim, col, explode, collect_set, count as spark_count, round as spark_round
from pyspark.sql.types import IntegerType, DoubleType, BooleanType
from pyspark.sql import SparkSession

def task04_quality_and_flags(spark, df, programs_df, output_dir):
    df = df.withColumn("typed_value",
                       when(col("valueType") == "BOOLEAN",
                            when(col("raw_value").isin("true","True","1"), True).when(col("raw_value").isin("false","False","0"), False).otherwise(None)
                       ).when(col("valueType") == "INTEGER_ZERO_OR_POSITIVE",
                              when(col("raw_value").rlike("^[0-9]+$"), col("raw_value").cast(IntegerType())).otherwise(None)
                       ).when(col("valueType").isin("NUMBER","PERCENTAGE"),
                              when(col("raw_value").rlike("^-?\\d+(\\.\\d+)?$"), col("raw_value").cast(DoubleType())).otherwise(None)
                       ).otherwise(None)
                      )
    df = df.withColumn("period_start", to_date(concat(substring(col("period"),1,4), lit("-"), substring(col("period"),5,2), lit("-01")))).withColumn("period_end", last_day(col("period_start")))
    df = df.withColumn("lastUpdated_ts", to_timestamp(col("lastUpdated"))).withColumn("lastUpdated_date", to_date(col("lastUpdated_ts")))
    df = df.withColumn("days_since_period_end", datediff(col("lastUpdated_date"), col("period_end")))
    df = df.withColumn("is_late_reported", when(col("days_since_period_end") > 60, True).otherwise(False)).withColumn("is_explicit_zero", when(col("raw_value") == "0", True).otherwise(False)).withColumn("is_missing_value", when(col("raw_value").isNull() | (trim(col("raw_value")) == ""), True).otherwise(False))
    # completeness by facility-period
    prog_expl = programs_df.select(explode(col("programs")).alias("p")).select(col("p.country").alias("country"), col("p.dataElements").alias("expected_dataElements"))
    expected_by_country = prog_expl.select(col("country"), explode(col("expected_dataElements")).alias("dataElement_uid")).dropDuplicates().groupBy("country").agg(spark_count("dataElement_uid").alias("expected_indicators"))
    reported = df.filter(col("typed_value").isNotNull()).groupBy("facility_uid","facility_name","country_name","period").agg(spark_count("dataElement_uid").alias("reported_count"), collect_set("dataElement_uid").alias("reported_set"))
    completeness = reported.join(expected_by_country, reported["country_name"] == expected_by_country["country"], how="left").withColumn("completeness_percent", when(col("expected_indicators").isNull(), None).otherwise(spark_round((col("reported_count")/col("expected_indicators"))*100,1)))
    completeness.coalesce(1).write.mode("overwrite").parquet(str(output_dir / "completeness_by_facility_period"))
    return df, completeness
