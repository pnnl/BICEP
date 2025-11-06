"""
Simple local data utilities for BICEP.

Since all database data is now provided as Parquet files by the data team,
this module only handles basic file operations for the parsed_inputs cache.
"""

import pandas as pd
from pathlib import Path
import logging

from utils.config import PARSED_INPUTS_PATH

logger = logging.getLogger(__name__)


def load_data_file(filename):
    """
    Load data file from parsed_inputs.
    
    Args:
        filename (str): Name of the file (without extension)
    
    Returns:
        pd.DataFrame: Loaded data
    """
    file_path = PARSED_INPUTS_PATH / f'{filename}.parquet'
    
    if not file_path.exists():
        logger.debug(f"Cache file not found: {file_path}")
        return pd.DataFrame()
    
    try:
        data = pd.read_parquet(file_path, engine='pyarrow')
        logger.debug(f"Loaded {len(data)} records from cache: {filename}")
        return data
    except Exception as e:
        logger.error(f"Failed to load file {filename}: {e}")
        return pd.DataFrame()


def save_data_file(filename, data):
    """
    Save data to parsed_inputs cache.
    
    Args:
        filename (str): Name for the saved file (without extension)
        data (pd.DataFrame): Data to save
        
    Returns:
        Path: Path to the saved file
    """
    # Ensure directory exists
    PARSED_INPUTS_PATH.mkdir(parents=True, exist_ok=True)
    
    output_path = PARSED_INPUTS_PATH / f'{filename}.parquet'
    data.to_parquet(output_path, engine='pyarrow', compression='snappy')
    
    logger.debug(f"Saved {len(data)} records to cache: {filename}")
    return output_path


def clear_cache():
    """Clear all files in the parsed_inputs cache directory."""
    if PARSED_INPUTS_PATH.exists():
        for file in PARSED_INPUTS_PATH.glob('*.parquet'):
            file.unlink()
        for file in PARSED_INPUTS_PATH.glob('*.json'):
            file.unlink()
        logger.info("Cleared parsed_inputs cache")
    else:
        logger.info("Cache directory does not exist")


if __name__ == '__main__':
    """
    Example usage of local_db_mirror.py:
    
    # Load cached data
    data = load_data_file('adoption_forecasts')
    
    # Save data to cache
    save_data_file('processed_data', dataframe)
    
    # Clear all cached files
    clear_cache()
    """
    pass