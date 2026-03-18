import pandas as pd
import numpy as np
from typing import Any, Dict, List
import re

from src.engines.base import BaseEngine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PandasEngine(BaseEngine):
    """Pandas implementation of the Execution Engine."""

    def load_data(self, source_config: Dict[str, Any]) -> pd.DataFrame:
        """Loads data into a Pandas DataFrame."""
        source_type = source_config.get("type", "").lower()
        path = source_config.get("path")

        logger.info(f"Loading data of type '{source_type}' from '{path}'")

        if source_type == "csv":
            df = pd.read_csv(path)
        elif source_type == "excel":
            df = pd.read_excel(path)
        elif source_type == "json":
            df = pd.read_json(path)
        else:
            raise ValueError(f"Unsupported source type for Pandas: {source_type}")

        logger.info(f"Loaded {len(df)} rows.")
        return df

    # --- Data Quality Checks ---
    def check_completeness(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        total_rows = len(df)
        null_count = df[column].isnull().sum()
        completeness_pct = ((total_rows - null_count) / total_rows) * 100 if total_rows > 0 else 100.0

        return {
            "status": "success",
            "total_rows": int(total_rows),
            "null_count": int(null_count),
            "completeness_pct": float(completeness_pct)
        }

    def check_uniqueness(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        total_rows = len(df)
        unique_count = df[column].nunique()
        duplicate_count = total_rows - unique_count
        uniqueness_pct = (unique_count / total_rows) * 100 if total_rows > 0 else 100.0

        return {
            "status": "success",
            "total_rows": int(total_rows),
            "unique_count": int(unique_count),
            "duplicate_count": int(duplicate_count),
            "uniqueness_pct": float(uniqueness_pct)
        }

    def check_range(self, df: pd.DataFrame, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        # Ignore nulls for range check
        valid_data = df[column].dropna()
        if not pd.api.types.is_numeric_dtype(valid_data):
            return {"status": "error", "error": f"Column '{column}' is not numeric."}

        out_of_range = valid_data[(valid_data < min_val) | (valid_data > max_val)]
        out_of_range_count = len(out_of_range)

        return {
            "status": "success",
            "total_valid_rows": int(len(valid_data)),
            "out_of_range_count": int(out_of_range_count),
            "out_of_range_pct": float((out_of_range_count / len(valid_data)) * 100) if len(valid_data) > 0 else 0.0
        }

    def check_regex(self, df: pd.DataFrame, column: str, pattern: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        valid_data = df[column].dropna().astype(str)
        # Match pattern
        matched = valid_data.str.match(pattern)
        mismatch_count = len(valid_data) - matched.sum()

        return {
            "status": "success",
            "total_valid_rows": int(len(valid_data)),
            "mismatch_count": int(mismatch_count),
            "mismatch_pct": float((mismatch_count / len(valid_data)) * 100) if len(valid_data) > 0 else 0.0
        }

    # --- Reconciliation Checks ---
    def get_row_count(self, df: pd.DataFrame) -> int:
        return len(df)

    def get_column_aggregates(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Dict[str, float]]:
        aggregates = {}
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                aggregates[col] = {
                    "sum": float(df[col].sum()),
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "avg": float(df[col].mean())
                }
        return aggregates

    def compare_records(self, source_df: pd.DataFrame, target_df: pd.DataFrame, primary_keys: List[str], mapping: Dict[str, str], tolerances: Dict[str, float]) -> Dict[str, Any]:
        """
        mapping: {source_col: target_col}
        tolerances: {source_col: numeric_tolerance_value}
        """
        # Rename target columns to match source for comparison
        target_renamed = target_df.rename(columns={v: k for k, v in mapping.items()})

        # Ensure primary keys exist
        missing_pk_source = [pk for pk in primary_keys if pk not in source_df.columns]
        missing_pk_target = [pk for pk in primary_keys if pk not in target_renamed.columns]
        
        if missing_pk_source or missing_pk_target:
             return {"status": "error", "error": f"Primary keys missing. Source: {missing_pk_source}, Target: {missing_pk_target}"}

        # Sort and set index for aligned comparison
        try:
            source_aligned = source_df.set_index(primary_keys).sort_index()
            target_aligned = target_renamed.set_index(primary_keys).sort_index()
        except KeyError as e:
             return {"status": "error", "error": f"Key error setting index: {e}"}

        # Find intersecting keys
        common_idx = source_aligned.index.intersection(target_aligned.index)
        source_only_idx = source_aligned.index.difference(target_aligned.index)
        target_only_idx = target_aligned.index.difference(source_aligned.index)

        comparison_results = {
            "common_records_count": len(common_idx),
            "source_only_records_count": len(source_only_idx),
            "target_only_records_count": len(target_only_idx),
            "mismatches": {}
        }

        # Compare common records column by column
        if len(common_idx) > 0:
            source_common = source_aligned.loc[common_idx]
            target_common = target_aligned.loc[common_idx]

            for col in mapping.keys():
                if col in source_common.columns and col in target_common.columns:
                    s_col = source_common[col]
                    t_col = target_common[col]

                    # Check for numeric tolerance
                    if col in tolerances and pd.api.types.is_numeric_dtype(s_col) and pd.api.types.is_numeric_dtype(t_col):
                        diff = (s_col - t_col).abs()
                        mismatches = diff > tolerances[col]
                    else:
                        mismatches = s_col != t_col
                        # Handle NaN == NaN case (which is False in Pandas)
                        mismatches = mismatches & ~(s_col.isna() & t_col.isna())

                    mismatch_count = int(mismatches.sum())
                    if mismatch_count > 0:
                        comparison_results["mismatches"][col] = {
                            "mismatch_count": mismatch_count,
                            "mismatch_pct": float((mismatch_count / len(common_idx)) * 100)
                        }

        return {"status": "success", "results": comparison_results}
