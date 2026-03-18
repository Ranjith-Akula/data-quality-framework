# Configuration-Driven Data Quality Framework

This is a modular, extensible, and JSON configuration-driven Data Quality and Reconciliation framework built with Python. It supports both **Pandas** (for small to medium datasets) and **PySpark** (for large, distributed datasets).

## Project Structure
```
data_quality_framework/
│
├── src/
│   ├── config/
│   │   └── parser.py        # Parses and validates JSON configurations
│   ├── core/
│   │   ├── validator.py     # Orchestrates DQ Validation rules
│   │   └── reconciler.py    # Orchestrates Source vs Target Reconciliation
│   ├── engines/
│   │   ├── base.py          # Abstract Base Engine interface
│   │   ├── pandas_engine.py # Pandas implementation
│   │   └── pyspark_engine.py# PySpark implementation
│   ├── reporting/
│   │   └── formatter.py     # Formats results into readable reports
│   └── utils/
│       └── logger.py        # Centralized logging
│
├── configs/                 # JSON configurations
│   ├── dq_validation_example.json
│   └── reconciliation_example.json
│
├── examples/                # Example execution scripts and dummy data
│   ├── run_validation.py
│   ├── run_reconciliation.py
│   └── ...
```

## Features

### 1. Source Data Quality Validation
Validates input data against predefined data quality dimensions defined in a JSON config.
- **Rules Supported:**
  - `completeness`: Checks for missing or null values.
  - `uniqueness`: Checks for duplicate values.
  - `range`: Checks if numeric values fall within a specified range (`min`, `max`).
  - `regex`: Checks if string values match a specified regular expression `pattern`.

### 2. Source vs Target Reconciliation
Compares source and target datasets.
- **Features Supported:**
  - `compare_row_count`: Compares total row counts.
  - `compare_aggregates`: Compares aggregations (sum, min, max, avg) for specified numeric columns.
  - `record_comparison`: Performs a row-by-row comparison based on `primary_keys`, with column `mapping`, and numeric `tolerances`.

## How to Run Examples

Make sure `pandas` and `pyspark` are installed:
```bash
pip install pandas pyspark numpy
```

Run Data Quality Validation:
```bash
python examples/run_validation.py
```

Run Source vs Target Reconciliation:
```bash
python examples/run_reconciliation.py
```

## Extending the Framework

**Adding a new Rule to Validation:**
1. Open `src/engines/base.py` and add the method signature for the new rule.
2. Implement the rule in `src/engines/pandas_engine.py` and `src/engines/pyspark_engine.py`.
3. Open `src/core/validator.py` and update the `execute` method to handle the new rule type from the JSON config.
4. Update `src/reporting/formatter.py` if the new rule produces custom output metrics.

**Adding a new Data Source Type:**
1. Update `load_data` in `src/engines/pandas_engine.py` (e.g., add support for SQL via SQLAlchemy).
2. Update `load_data` in `src/engines/pyspark_engine.py` (e.g., add support for Avro or a specific JDBC dialect).

**Supporting a New Engine (e.g., Polars or Dask):**
1. Create `src/engines/polars_engine.py`.
2. Implement the class `PolarsEngine(BaseEngine)` overriding all abstract methods.
3. Instantiate and pass this new engine to the `Validator` or `Reconciler`.
