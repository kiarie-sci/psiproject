from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from typing import Dict

CONTRACT = {
    "dataElement_uid": {"type": "STRING", "nullable": False},
    "dataElement_name": {"type": "STRING", "nullable": False},
    "orgUnit_uid": {"type": "STRING", "nullable": False},
    "facility_uid": {"type": "STRING", "nullable": False},
    "period": {"type": "STRING", "nullable": False},
    "year_month": {"type": "STRING", "nullable": False},
    "typed_value": {"type": "DOUBLE", "nullable": True},
    "is_late_reported": {"type": "BOOLEAN", "nullable": False},
}

def validate_contract(df: DataFrame, contract: Dict = CONTRACT):
    missing = [c for c in contract.keys() if c not in df.columns]
    if missing:
        raise AssertionError(f"Missing contract columns: {missing}")
    non_nullable = [c for c, spec in contract.items() if not spec.get("nullable", True)]
    for c in non_nullable:
        cnt = df.filter(col(c).isNull()).limit(1).count()
        if cnt > 0:
            raise AssertionError(f"Column {c} has nulls")
    # Basic type heuristics omitted for brevity; if needed, add casting checks.
    return True
