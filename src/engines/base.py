from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseEngine(ABC):
    """
    Abstract Base Class for Execution Engines.
    Defines the interface for data loading, quality checks, and reconciliation.
    """

    @abstractmethod
    def load_data(self, source_config: Dict[str, Any]) -> Any:
        """Loads data from a given source configuration."""
        pass

    # --- Data Quality Checks ---
    @abstractmethod
    def check_completeness(self, df: Any, column: str) -> Dict[str, Any]:
        """Checks for null or missing values in a column."""
        pass

    @abstractmethod
    def check_uniqueness(self, df: Any, column: str) -> Dict[str, Any]:
        """Checks for duplicate values in a column."""
        pass

    @abstractmethod
    def check_range(self, df: Any, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
        """Checks if numeric values fall within a specified range."""
        pass

    @abstractmethod
    def check_regex(self, df: Any, column: str, pattern: str) -> Dict[str, Any]:
        """Checks if string values match a specified regular expression."""
        pass

    # --- Reconciliation Checks ---
    @abstractmethod
    def get_row_count(self, df: Any) -> int:
        """Returns the total number of rows in the dataframe."""
        pass

    @abstractmethod
    def get_column_aggregates(self, df: Any, columns: List[str]) -> Dict[str, Dict[str, float]]:
        """Returns aggregates (sum, min, max, avg) for numeric columns."""
        pass

    @abstractmethod
    def compare_records(self, source_df: Any, target_df: Any, primary_keys: List[str], mapping: Dict[str, str], tolerances: Dict[str, float]) -> Dict[str, Any]:
        """Performs a row-by-row comparison between source and target dataframes."""
        pass
