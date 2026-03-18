from typing import Any, Dict, List
from src.engines.base import BaseEngine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Reconciler:
    """
    Orchestrates Source vs Target Reconciliation based on the provided configuration.
    """

    def __init__(self, engine: BaseEngine):
        """
        Initializes the Reconciler with a specific execution engine.
        """
        self.engine = engine

    def execute(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the Reconciliation rules defined in the config.

        Args:
            config: The Reconciliation configuration dictionary.

        Returns:
            A dictionary containing the reconciliation results.
        """
        logger.info("Starting Data Reconciliation...")

        try:
            source_df = self.engine.load_data(config['source'])
            target_df = self.engine.load_data(config['target'])
        except Exception as e:
             logger.error(f"Failed to load data for reconciliation: {e}")
             return {"status": "failed", "error": f"Failed to load data: {e}"}

        recon_config = config.get('reconciliation', {})
        results = {
            "status": "success",
            "reconciliation_results": {}
        }

        # 1. Row Count Comparison
        if recon_config.get('compare_row_count', False):
            logger.info("Comparing row counts")
            source_count = self.engine.get_row_count(source_df)
            target_count = self.engine.get_row_count(target_df)
            diff = abs(source_count - target_count)
            
            results['reconciliation_results']['row_count'] = {
                "source_count": source_count,
                "target_count": target_count,
                "difference": diff,
                "percentage_diff": float((diff / source_count) * 100) if source_count > 0 else 0.0,
                "match": source_count == target_count
            }

        # 2. Column Aggregates Comparison
        aggregate_cols = recon_config.get('compare_aggregates', [])
        if aggregate_cols:
             logger.info(f"Comparing aggregates for columns: {aggregate_cols}")
             source_aggs = self.engine.get_column_aggregates(source_df, aggregate_cols)
             target_aggs = self.engine.get_column_aggregates(target_df, aggregate_cols)

             agg_results = {}
             for col in aggregate_cols:
                 if col in source_aggs and col in target_aggs:
                     s_agg = source_aggs[col]
                     t_agg = target_aggs[col]
                     agg_results[col] = {
                         "source": s_agg,
                         "target": t_agg,
                         "differences": {
                             "sum": abs(s_agg["sum"] - t_agg["sum"]),
                             "avg": abs(s_agg["avg"] - t_agg["avg"])
                         }
                     }
             results['reconciliation_results']['aggregates'] = agg_results

        # 3. Record Level Comparison
        record_comparison = recon_config.get('record_comparison')
        if record_comparison:
             logger.info("Performing record-level comparison")
             primary_keys = record_comparison.get('primary_keys', [])
             mapping = record_comparison.get('mapping', {})
             tolerances = record_comparison.get('tolerances', {})

             if not primary_keys:
                 logger.error("Primary keys are required for record-level comparison.")
                 results['reconciliation_results']['record_comparison'] = {"status": "error", "error": "Primary keys missing"}
             else:
                 res = self.engine.compare_records(source_df, target_df, primary_keys, mapping, tolerances)
                 results['reconciliation_results']['record_comparison'] = res

        logger.info("Finished Data Reconciliation.")
        return results
