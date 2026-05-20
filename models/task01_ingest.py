from pathlib import Path
from typing import Tuple, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, isnull
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, BooleanType, IntegerType
)
# Schema for data_values.sjon
DATA_VALUES_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("dataValues", ArrayType(
        StructType([
            StructField("dataElement", StringType(), True),
            StructField("period", StringType(), True),
            StructField("orgUnit", StringType(), True),
            StructField("categoryOptionCombo", StringType(), True),
            StructField("attributeOptionCombo", StringType(), True),
            StructField("value", StringType(), True),
            StructField("storedBy", StringType(), True),
            StructField("created", StringType(), True),
            StructField("lastUpdated", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("followup", BooleanType(), True)
        ])
    ), True)
])

METADATA_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("dataElements", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("shortName", StringType(), True),
            StructField("code", StringType(), True),
            StructField("valueType", StringType(), True),
            StructField("domainType", StringType(), True),
            StructField("aggregationType", StringType(), True),
            StructField("zeroIsSignificant", BooleanType(), True),
            StructField("categoryCombo", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("dataElementGroups", ArrayType(
                StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True)
                ])
            ), True),
            StructField("created", StringType(), True),
            StructField("lastUpdated", StringType(), True)
        ])
    ), True),
    StructField("categoryOptionCombos", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("created", StringType(), True),
            StructField("lastUpdated", StringType(), True)
        ])
    ), True)
])

ORG_UNITS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("organisationUnits", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("shortName", StringType(), True),
            StructField("code", StringType(), True),
            StructField("level", IntegerType(), True),
            StructField("path", StringType(), True),
            StructField("parent", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True)
            ]), True),
            StructField("groups", ArrayType(
                StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True)
                ])
            ), True),
            StructField("created", StringType(), True),
            StructField("lastUpdated", StringType(), True)
        ])
    ), True)
])

PROGRAMS_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("version", StringType(), True),
    StructField("programs", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("shortName", StringType(), True),
            StructField("healthArea", StringType(), True),
            StructField("country", StringType(), True),
            StructField("reportingFrequency", StringType(), True),
            StructField("dataElements", ArrayType(StringType()), True),
            StructField("created", StringType(), True),
            StructField("lastUpdated", StringType(), True)
        ])
    ), True)
])


def task01_ingest_and_flatten(spark: SparkSession, data_dir: Path) -> Tuple:
    """
    Loads data_values.json, metadata.json, org_units.json, programs.json using
    explicit schemas, explodes dataValues[], quarantines rows missing critical fields,
    and returns (valid_df, metadata_df, org_units_df, programs_df, stats).
    """
    # input paths
    data_values_path = data_dir / "data_values.json"
    metadata_path = data_dir / "metadata.json"
    org_units_path = data_dir / "org_units.json"
    programs_path = data_dir / "programs.json"

    # load with explicit schemas
    data_values_df = spark.read.schema(DATA_VALUES_SCHEMA).json(str(data_values_path))
    metadata_df = spark.read.schema(METADATA_SCHEMA).json(str(metadata_path))
    org_units_df = spark.read.schema(ORG_UNITS_SCHEMA).json(str(org_units_path))
    programs_df = spark.read.schema(PROGRAMS_SCHEMA).json(str(programs_path))

    # explode dataValues -> one row per data value
    exploded = data_values_df.select(explode(col("dataValues")).alias("dv"))
    flattened = exploded.select(
        col("dv.dataElement").alias("dataElement_uid"),
        col("dv.period").alias("period"),
        col("dv.orgUnit").alias("orgUnit_uid"),
        col("dv.categoryOptionCombo").alias("categoryOptionCombo_uid"),
        col("dv.value").alias("raw_value"),
        col("dv.storedBy").alias("storedBy"),
        col("dv.created").alias("created"),
        col("dv.lastUpdated").alias("lastUpdated"),
        col("dv.comment").alias("comment"),
        col("dv.followup").alias("followup")
    )

    total = flattened.count()

    # quarantine rows missing critical fields
    quarantine = flattened.filter(
        isnull(col("dataElement_uid")) | isnull(col("period")) | isnull(col("orgUnit_uid"))
    )
    qcnt = quarantine.count()

    # write quarantine if any (default: data_dir.parent/output/quarantine)
    out_quarantine = data_dir.parent / "output" / "quarantine"
    out_quarantine.mkdir(parents=True, exist_ok=True)
    if qcnt > 0:
        quarantine.coalesce(1).write.mode("overwrite").parquet(str(out_quarantine / "schema_violations"))

    # keep valid rows
    valid = flattened.exceptAll(quarantine)

    stats: Dict[str, Any] = {
        "exploded_total": total,
        "quarantined": qcnt,
        "valid": valid.count(),
        "metadata_rows": metadata_df.count(),
        "org_units_rows": org_units_df.count(),
        "programs_rows": programs_df.count()
    }

    return valid, metadata_df, org_units_df, programs_df, stats
