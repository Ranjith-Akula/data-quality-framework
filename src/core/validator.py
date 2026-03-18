from typing import Any, Dict, List
from src.engines.base import BaseEngine
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Validator:
    """
    Orchestrates Data Quality Validation based on the provided configuration.
    """

    def __init__(self, engine: BaseEngine):
        """
        Initializes the Validator with a specific execution engine.
        """
        self.engine = engine

    def execute(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes the DQ rules defined in the config.

        Args:
            config: The Data Quality configuration dictionary.

        Returns:
            A dictionary containing the results of all rule evaluations.
        """
        dataset_name = config.get('dataset_name', 'Unknown')
        logger.info(f"Starting Data Quality Validation for dataset: {dataset_name}")

        try:
            df = self.engine.load_data(config['source'])
        except Exception as e:
            logger.error(f"Failed to load data for dataset {dataset_name}: {e}")
            return {"dataset": dataset_name, "status": "failed", "error": str(e)}

        results = {
            "dataset": dataset_name,
            "status": "success",
            "rule_results": []
        }

        rules = config.get('rules', [])
        for rule in rules:
            rule_type = rule.get('type')
            rule_name = rule.get('name', rule_type)
            column = rule.get('column')

            logger.info(f"Executing rule '{rule_name}' of type '{rule_type}' on column '{column}'")

            try:
                if rule_type == 'completeness':
                    res = self.engine.check_completeness(df, column)
                elif rule_type == 'uniqueness':
                    res = self.engine.check_uniqueness(df, column)
                elif rule_type == 'range':
                    min_val = rule.get('min')
                    max_val = rule.get('max')
                    res = self.engine.check_range(df, column, min_val, max_val)
                elif rule_type == 'regex':
                    pattern = rule.get('pattern')
                    res = self.engine.check_regex(df, column, pattern)
                else:
                    logger.warning(f"Unsupported rule type: {rule_type}")
                    res = {"status": "error", "error": f"Unsupported rule type: {rule_type}"}
                
                rule_result = {
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "column": column,
                    "result": res
                }
                
                # Evaluate against threshold if provided
                threshold = rule.get('threshold')
                if threshold is not None and res.get('status') == 'success':
                    # Depending on rule type, define what success means
                    passed = True
                    if rule_type == 'completeness':
                        passed = res['completeness_pct'] >= threshold
                    elif rule_type == 'uniqueness':
                        passed = res['uniqueness_pct'] >= threshold
                    elif rule_type == 'range':
                        passed = res['out_of_range_pct'] <= threshold
                    elif rule_type == 'regex':
                        passed = res['mismatch_pct'] <= threshold
                        
                    rule_result['passed'] = passed
                    rule_result['threshold'] = threshold

                results['rule_results'].append(rule_result)

            except Exception as e:
                logger.error(f"Error executing rule '{rule_name}': {e}")
                results['rule_results'].append({
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "column": column,
                    "result": {"status": "error", "error": str(e)}
                })
                results["status"] = "partial_failure"

        logger.info(f"Finished validation for dataset: {dataset_name}")
        return results
