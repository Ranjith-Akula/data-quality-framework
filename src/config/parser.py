import json
from pathlib import Path
from typing import Dict, Any
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ConfigParser:
    """
    Parses and validates JSON configuration files for the Data Quality Framework.
    """
    
    @staticmethod
    def load_config(file_path: str) -> Dict[str, Any]:
        """
        Loads a JSON configuration file.
        
        Args:
            file_path: Path to the JSON configuration file.
            
        Returns:
            A dictionary containing the configuration.
            
        Raises:
            FileNotFoundError: If the file is not found.
            json.JSONDecodeError: If the file is not valid JSON.
        """
        logger.info(f"Loading configuration from {file_path}")
        path = Path(file_path)
        
        if not path.exists():
            logger.error(f"Configuration file not found: {file_path}")
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
            
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info("Configuration loaded successfully")
            return config
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration: {e}")
            raise
    
    @staticmethod
    def validate_dq_config(config: Dict[str, Any]) -> None:
        """
        Validates a Data Quality configuration.
        """
        required_keys = ['source', 'rules']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key in DQ config: '{key}'")
                
        if not isinstance(config.get('rules'), list):
            raise ValueError("'rules' must be a list of rule definitions")

    @staticmethod
    def validate_recon_config(config: Dict[str, Any]) -> None:
        """
        Validates a Reconciliation configuration.
        """
        required_keys = ['source', 'target', 'reconciliation']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required key in Reconciliation config: '{key}'")
