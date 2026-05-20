#!/usr/bin/env python3
"""
Entry-point orchestrator. Runs Tasks 01-07 in order and performs DQ checks.
Usage:
    python pipeline.py --data-dir ./data --output-dir ./output
"""
import argparse
import logging
import sys
from pathlib import Path
from pyspark.sql import SparkSession

from models.task01_ingest import task01_ingest_and_flatten
from models.task02_metadata import task02_resolve_metadata
from models.task03_hierarchy import task03_resolve_hierarchy
from models.task04_quality import task04_quality_and_flags
from models.task05_dimensional import task05_build_dimensions_and_fact
from models.task06_analytics import task06_program_analytics
from models.task07_cross_country import task07_cross_country_aggregation
from models.contract import validate_contract  # B1 validator
from models.task02_metadata import extract_de_lookup  # helper for dim

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline")

def get_spark():
    return SparkSession.builder.master("local[*]").appName("DHIS2-Pipeline").config("spark.driver.memory", "4g").getOrCreate()

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data-dir", required=True, help="Path to data directory")
    p.add_argument("--output-dir", required=True, help="Path to output directory")
    p.add_argument("--enable-b1", action="store_true", help="Enable data contract validation (B1)")
    p.add_argument("--enable-b2", action="store_true", help="Enable incremental load (B2)")
    p.add_argument("--enable-b3", action="store_true", help="Enable anomaly detection (B3)")
    p.add_argument("--enable-b4", action="store_true", help="Enable metadata drift (B4)")
    return p.parse_args()

def main():
    args = parse_args()
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Task 01
        valid_df, meta_df, ou_df, prog_df, t1_stats = task01_ingest_and_flatten(spark, data_dir)
        logger.info(f"Task01 finished: {t1_stats}")

        # Task 02
        joined_df, unresolved_stats = task02_resolve_metadata(spark, valid_df, meta_df, ou_df, output_dir)
        logger.info(f"Task02 finished: {unresolved_stats}")

        # Task 03
        hier_df = task03_resolve_hierarchy(spark, joined_df, ou_df)
        logger.info("Task03 finished")

        # Task 04
        quality_df, completeness_df = task04_quality_and_flags(spark, hier_df, prog_df, output_dir)
        logger.info("Task04 finished")

        # Task 05
        fact_df = task05_build_dimensions_and_fact(spark, quality_df, meta_df, prog_df, output_dir, incremental=args.enable_b2)
        logger.info("Task05 finished")

        # Optional B1: validate fact contract
        if args.enable_b1:
            logger.info("Running B1 data contract validation")
            validate_contract(fact_df)

        # Task 06
        task06_program_analytics(spark, fact_df, output_dir)
        logger.info("Task06 finished")

        # Task 07
        task07_cross_country_aggregation(spark, fact_df, completeness_df, output_dir)
        logger.info("Task07 finished")

        # DQ checks
        quarantine_rate = t1_stats.get("quarantined", 0) / max(1, t1_stats.get("exploded_total", 1))
        if quarantine_rate > 0.10:
            logger.error("Critical DQ: quarantine_rate > 10%")
            sys.exit(2)

        if fact_df.count() == 0:
            logger.error("Critical DQ: fact table is empty")
            sys.exit(2)

        logger.info("Pipeline completed successfully")
        sys.exit(0)

    except Exception as exc:
        logger.exception("Pipeline failed")
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

