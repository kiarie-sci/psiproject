from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, broadcast
from pathlib import Path

def extract_de_lookup(meta_df):
    return meta_df.select(explode(col("dataElements")).alias("de")).select(col("de.id").alias("dataElement_uid"), col("de.name").alias("dataElement_name"), col("de.valueType").alias("valueType")).dropDuplicates()

def task02_resolve_metadata(spark, valid_df, meta_df, ou_df, output_dir: Path):
    de_lookup = extract_de_lookup(meta_df)
    coc_lookup = meta_df.select(explode(col("categoryOptionCombos")).alias("coc")).select(col("coc.id").alias("categoryOptionCombo_uid"), col("coc.name").alias("categoryOptionCombo_name")).dropDuplicates()
    ou_lookup = ou_df.select(explode(col("organisationUnits")).alias("ou")).select(col("ou.id").alias("orgUnit_uid"), col("ou.name").alias("orgUnit_name"), col("ou.level").alias("orgUnit_level"), col("ou.path").alias("orgUnit_path")).dropDuplicates()

    joined = valid_df.join(broadcast(de_lookup), on="dataElement_uid", how="left") \
                     .join(broadcast(coc_lookup), on="categoryOptionCombo_uid", how="left") \
                     .join(broadcast(ou_lookup), on="orgUnit_uid", how="left")

    unresolved_de = valid_df.join(de_lookup.select("dataElement_uid"), on="dataElement_uid", how="left_anti")
    unresolved_coc = valid_df.join(coc_lookup.select("categoryOptionCombo_uid"), on="categoryOptionCombo_uid", how="left_anti")
    unresolved_ou = valid_df.join(ou_lookup.select("orgUnit_uid"), on="orgUnit_uid", how="left_anti")

    # write unresolved to output_dir/unresolved
    unresolved_dir = output_dir / "unresolved"
    unresolved_dir.mkdir(parents=True, exist_ok=True)
    if unresolved_de.count() > 0:
        unresolved_de.coalesce(1).write.mode("overwrite").parquet(str(unresolved_dir / "data_elements"))
    if unresolved_coc.count() > 0:
        unresolved_coc.coalesce(1).write.mode("overwrite").parquet(str(unresolved_dir / "category_option_combos"))
    if unresolved_ou.count() > 0:
        unresolved_ou.coalesce(1).write.mode("overwrite").parquet(str(unresolved_dir / "org_units"))

    stats = {"unresolved_de": unresolved_de.count(), "unresolved_coc": unresolved_coc.count(), "unresolved_ou": unresolved_ou.count()}
    return joined, stats
