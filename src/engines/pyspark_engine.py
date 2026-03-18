from typing import Any, Dict, List
import pyspark.sql.functions as F
from pyspark.sql.types import NumericType
from src.engines.base import BaseEngine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class PySparkEngine(BaseEngine):
    """PySpark implementation of the Execution Engine."""

    def __init__(self, spark_session: Any):
        """
        Initializes the engine with a SparkSession.
        """
        self.spark = spark_session

    def load_data(self, source_config: Dict[str, Any]) -> Any:
        """Loads data into a PySpark DataFrame."""
        source_type = source_config.get("type", "").lower()
        path = source_config.get("path")
        options = source_config.get("options", {})

        logger.info(f"Loading data of type '{source_type}' from '{path}'")

        if source_type == "csv":
            reader = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true")
        elif source_type == "json":
            reader = self.spark.read.format("json")
        elif source_type == "parquet":
            reader = self.spark.read.format("parquet")
        elif source_type == "jdbc":
            reader = self.spark.read.format("jdbc")
        else:
            raise ValueError(f"Unsupported source type for PySpark: {source_type}")

        for k, v in options.items():
            reader = reader.option(k, v)

        df = reader.load(path)
        logger.info(f"Data schema loaded successfully.")
        return df

    # --- Data Quality Checks ---
    def check_completeness(self, df: Any, column: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        total_rows = df.count()
        null_count = df.filter(F.col(column).isNull()).count()
        completeness_pct = ((total_rows - null_count) / total_rows) * 100 if total_rows > 0 else 100.0

        return {
            "status": "success",
            "total_rows": total_rows,
            "null_count": null_count,
            "completeness_pct": float(completeness_pct)
        }

    def check_uniqueness(self, df: Any, column: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        total_rows = df.count()
        unique_count = df.select(column).distinct().count()
        duplicate_count = total_rows - unique_count
        uniqueness_pct = (unique_count / total_rows) * 100 if total_rows > 0 else 100.0

        return {
            "status": "success",
            "total_rows": total_rows,
            "unique_count": unique_count,
            "duplicate_count": duplicate_count,
            "uniqueness_pct": float(uniqueness_pct)
        }

    def check_range(self, df: Any, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        field = next((f for f in df.schema.fields if f.name == column), None)
        if not isinstance(field.dataType, NumericType):
            return {"status": "error", "error": f"Column '{column}' is not numeric."}

        valid_data = df.filter(F.col(column).isNotNull())
        total_valid_rows = valid_data.count()
        
        out_of_range = valid_data.filter((F.col(column) < min_val) | (F.col(column) > max_val))
        out_of_range_count = out_of_range.count()

        return {
            "status": "success",
            "total_valid_rows": total_valid_rows,
            "out_of_range_count": out_of_range_count,
            "out_of_range_pct": float((out_of_range_count / total_valid_rows) * 100) if total_valid_rows > 0 else 0.0
        }

    def check_regex(self, df: Any, column: str, pattern: str) -> Dict[str, Any]:
        if column not in df.columns:
            return {"status": "error", "error": f"Column '{column}' not found."}

        valid_data = df.filter(F.col(column).isNotNull())
        total_valid_rows = valid_data.count()

        matched = valid_data.filter(F.col(column).cast("string").rlike(pattern))
        matched_count = matched.count()
        mismatch_count = total_valid_rows - matched_count

        return {
            "status": "success",
            "total_valid_rows": total_valid_rows,
            "mismatch_count": mismatch_count,
            "mismatch_pct": float((mismatch_count / total_valid_rows) * 100) if total_valid_rows > 0 else 0.0
        }

    # --- Reconciliation Checks ---
    def get_row_count(self, df: Any) -> int:
        return df.count()

    def get_column_aggregates(self, df: Any, columns: List[str]) -> Dict[str, Dict[str, float]]:
        aggregates = {}
        for col in columns:
            if col in df.columns:
                field = next((f for f in df.schema.fields if f.name == col), None)
                if isinstance(field.dataType, NumericType):
                    agg_row = df.select(
                        F.sum(col).alias("sum"),
                        F.min(col).alias("min"),
                        F.max(col).alias("max"),
                        F.avg(col).alias("avg")
                    ).collect()[0]
                    aggregates[col] = {
                        "sum": float(agg_row["sum"]) if agg_row["sum"] is not None else 0.0,
                        "min": float(agg_row["min"]) if agg_row["min"] is not None else 0.0,
                        "max": float(agg_row["max"]) if agg_row["max"] is not None else 0.0,
                        "avg": float(agg_row["avg"]) if agg_row["avg"] is not None else 0.0
                    }
        return aggregates

    def compare_records(self, source_df: Any, target_df: Any, primary_keys: List[str], mapping: Dict[str, str], tolerances: Dict[str, float]) -> Dict[str, Any]:
        # Rename target columns based on mapping
        target_renamed = target_df
        for s_col, t_col in mapping.items():
            if t_col in target_renamed.columns:
                 target_renamed = target_renamed.withColumnRenamed(t_col, s_col)

        # Check for missing PKs
        missing_pk_source = [pk for pk in primary_keys if pk not in source_df.columns]
        missing_pk_target = [pk for pk in primary_keys if pk not in target_renamed.columns]
        
        if missing_pk_source or missing_pk_target:
             return {"status": "error", "error": f"Primary keys missing. Source: {missing_pk_source}, Target: {missing_pk_target}"}

        # Join on PKs (Full Outer Join to find matches and mismatches)
        join_conditions = [source_df[pk] == target_renamed[pk] for pk in primary_keys]
        joined_df = source_df.alias("s").join(target_renamed.alias("t"), on=primary_keys, how="full_outer")

        # Cache joined dataframe for multiple aggregations
        joined_df.cache()

        # Count records
        s_pk = F.col(f"s.{primary_keys[0]}") if primary_keys else F.lit(None)
        t_pk = F.col(f"t.{primary_keys[0]}") if primary_keys else F.lit(None)
        
        # It's a bit tricky to check source only/target only this way due to join key deduplication in PySpark
        # Let's perform left anti joins instead for accuracy
        source_only_count = source_df.join(target_renamed, on=primary_keys, how="left_anti").count()
        target_only_count = target_renamed.join(source_df, on=primary_keys, how="left_anti").count()
        common_records_df = source_df.alias("s").join(target_renamed.alias("t"), on=primary_keys, how="inner")
        common_records_count = common_records_df.count()

        comparison_results = {
            "common_records_count": common_records_count,
            "source_only_records_count": source_only_count,
            "target_only_records_count": target_only_count,
            "mismatches": {}
        }

        if common_records_count > 0:
            for col in mapping.keys():
                s_col_ref = F.col(f"s.{col}")
                t_col_ref = F.col(f"t.{col}")

                try:
                    if col in tolerances:
                        tol = tolerances[col]
                        mismatches_df = common_records_df.filter(
                            (s_col_ref.isNotNull() & t_col_ref.isNotNull() & (F.abs(s_col_ref - t_col_ref) > tol)) |
                            (s_col_ref.isNotNull() & t_col_ref.isNull()) |
                            (s_col_ref.isNull() & t_col_ref.isNotNull())
                        )
                    else:
                        mismatches_df = common_records_df.filter(
                            (s_col_ref != t_col_ref) |
                            (s_col_ref.isNotNull() & t_col_ref.isNull()) |
                            (s_col_ref.isNull() & t_col_ref.isNotNull())
                        )
                    
                    mismatch_count = mismatches_df.count()
                    if mismatch_count > 0:
                         comparison_results["mismatches"][col] = {
                             "mismatch_count": mismatch_count,
                             "mismatch_pct": float((mismatch_count / common_records_count) * 100)
                         }
                except Exception as e:
                    logger.error(f"Error comparing column '{col}': {e}")
        
        joined_df.unpersist()
        return {"status": "success", "results": comparison_results}
