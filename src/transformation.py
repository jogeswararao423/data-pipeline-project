"""
Data Transformation and Normalization Module
Standardizes formats, normalizes timestamps and currency decimals,
and extracts affected business dates for incremental recomputation.
"""

from decimal import Decimal
from typing import Set, List
import pandas as pd


def normalize_event_dataframe(df_valid: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column formats, trims string fields, formats dates,
    and guarantees exact 2-decimal financial values.
    """
    if df_valid.empty:
        return df_valid.copy()

    df = df_valid.copy()
    
    # Text normalizations
    str_cols = ["event_id", "order_id", "customer_id", "product_id", "event_type", "source_file"]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # Datetime and Business Date standardizations
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["business_date"] = pd.to_datetime(df["business_date"]).dt.strftime("%Y-%m-%d")

    # Numeric and financial standardizations
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].apply(lambda x: float(Decimal(str(x)).quantize(Decimal("0.01"))))
    df["total_amount"] = df["total_amount"].apply(lambda x: float(Decimal(str(x)).quantize(Decimal("0.01"))))
    df["source_version"] = df["source_version"].astype(int)

    return df


def extract_affected_business_dates(df: pd.DataFrame) -> List[str]:
    """Returns sorted unique business dates affected by the current batch of events."""
    if df.empty or "business_date" not in df.columns:
        return []
    unique_dates = sorted(df["business_date"].unique().tolist())
    return unique_dates
