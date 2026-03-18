import sys
import os

# Add the project root to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config.parser import ConfigParser
from src.engines.pandas_engine import PandasEngine
from src.core.validator import Validator
from src.reporting.formatter import ReportFormatter

def main():
    config_path = "../configs/dq_validation_example.json"
    
    print("1. Loading Configuration")
    config = ConfigParser.load_config(config_path)
    ConfigParser.validate_dq_config(config)
    
    print("\n2. Initializing Engine (Pandas)")
    engine = PandasEngine()
    
    print("\n3. Executing Validation")
    validator = Validator(engine)
    results = validator.execute(config)
    
    print("\n4. Generating Report\n")
    report = ReportFormatter.format_validation_report(results)
    print(report)

if __name__ == "__main__":
    main()
