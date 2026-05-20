# DHIS2 PySpark Pipeline

Overview
--------
This repository contains a complete PySpark pipeline showing how to transform data as received from the DHIS2, to make it usable for the DISC team at PSI.
The project is done in the python django framework, dor easy packaging and deployment, but the core of the project is the PySpark code in `pipeline.py` which contains the main data transformation logic.

It includes:
- Task 01 — JSON ingestion & schema flattening
- Task 02 — Metadata UID resolution
- Task 03 — Org unit hierarchy resolution
- Task 04 — Data quality & late-reporting flags
- Task 05 — Dimensional model build
- Task 06 — Program analytics & window functions
- Task 07 — Cross-country aggregation
- Task 08 — Pipeline orchestration

Bonuses implemented:
- B1 — Data contract validation (pytest tests)
- B2 — Incremental load detection (by year_month partitions)
- B3 — Anomaly detection (3σ vs 12-month rolling mean)
- B4 — Metadata drift detection (snapshot compare)



```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
