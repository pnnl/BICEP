"""
BICEP Pipeline Configuration

Centralized configuration for all file paths, directories, and settings used throughout
the BICEP pipeline. This ensures consistent path handling across all modules.
"""

from pathlib import Path
import os
from loguru import logger

# ============= BASE DIRECTORIES =============

# Root directory of the BICEP project (utils/../ = project root)
BICEP_ROOT = Path(__file__).parent.parent
ROOT = BICEP_ROOT  # Keep Tim's variable name for compatibility

# Data directories
DATA_ROOT = BICEP_ROOT / 'data'
DATA_DIR = DATA_ROOT  # Keep Tim's variable name for compatibility
RAW_INPUTS_PATH = DATA_ROOT / 'raw_inputs'
PARSED_INPUTS_PATH = DATA_ROOT / 'parsed_inputs'
REQUIRED_INPUT_PATH = DATA_ROOT / 'required_input'

# ============= DATA LOCATION SETTING =============

DATA_LOCATION = 'LOCAL'
try:
    assert DATA_LOCATION in ('LOCAL', 'PNNL Database')
except AssertionError as error:
    logger.error(f'DATA_LOCATION {DATA_LOCATION} is not valid. Must be in ["LOCAL", "PNNL Database"]')
    logger.error(error)
    raise AssertionError

# ============= INPUT FILES =============

# Scout scenario files
SCOUT_BAU_FILE = RAW_INPUTS_PATH / 'Scout_ref_scenario.json'
SCOUT_HIGH_FILE = RAW_INPUTS_PATH / 'Scout_high_scenario.json'

# EV projection files
EV_PROJECTIONS_FILE = RAW_INPUTS_PATH / 'TEMPO_LDV_EV_county_stock_projections.csv'

# PV projection files
PV_PROJECTIONS_FILE = RAW_INPUTS_PATH / 'ReEDS-distpv_cap-bau_high.csv'

# Hierarchy and mapping files
HIERARCHY_FILE = RAW_INPUTS_PATH / 'hierarchy.csv'
TECHNOLOGY_MAP_FILE = BICEP_ROOT / 'technology_map.csv'

# Required input files
COST_FACTOR_FILE = REQUIRED_INPUT_PATH / 'cost_factor.csv'

# GitHub data assets that will be downloaded automatically if not present
BICEP_DATA_ASSETS = ['https://github.com/pnnl/BICEP/releases/download/v0.1-data/adoption-forecasts.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/load-diff.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/peak-load.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/scout-xstock-tech-mapping.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/stock-meta.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/technologies.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/upgrades.parquet',
                     'https://github.com/pnnl/BICEP/releases/download/v0.1-data/bicep.x-stock.db'
                     ]

# Local database file path
LOCAL_DB_FILE = DATA_ROOT / 'bicep.x-stock.db'


def download_data_assets():
    """
    Download required data assets from GitHub releases if they don't exist locally.
    Includes built-in retry logic with exponential backoff for temporary failures.
    """
    import urllib.request
    import time
    from pathlib import Path
    
    max_retries = 3
    base_delay = 2  # seconds
    
    for asset_url in BICEP_DATA_ASSETS:
        filename = asset_url.split('/')[-1]
        local_path = DATA_ROOT / filename
        
        if not local_path.exists():
            logger.info(f"Downloading {filename} from GitHub releases...")
            
            for attempt in range(max_retries + 1):
                try:
                    urllib.request.urlretrieve(asset_url, local_path)
                    logger.info(f"✓ Downloaded {filename}")
                    break  # Success, move to next file
                except Exception as e:
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"⚠ Download attempt {attempt + 1} failed for {filename}: {e}")
                        logger.info(f"Retrying in {delay} seconds... ({attempt + 1}/{max_retries} retries)")
                        time.sleep(delay)
                    else:
                        logger.error(f"✗ Failed to download {filename} after {max_retries + 1} attempts: {e}")
                        logger.error(f"This file may be temporarily unavailable. The system will retry when needed.")
        else:
            logger.debug(f"✓ {filename} already exists locally")


def ensure_data_assets():
    """
    Ensure all required data assets are available locally.
    Downloads them if missing with built-in retry logic.
    
    This function implements a robust recovery mechanism that:
    1. Checks for missing files multiple times
    2. Retries downloads with exponential backoff
    3. Continues execution even if some files temporarily fail
    4. Re-attempts missing files when called again later
    """
    # Check if database file exists
    if not LOCAL_DB_FILE.exists():
        logger.info("Local database file not found, downloading required data assets...")
        download_data_assets()
    else:
        logger.debug("Local database file exists")
        
    # Verify all assets are present (check up to 2 times for robustness)
    for check_attempt in range(2):
        missing_assets = []
        for asset_url in BICEP_DATA_ASSETS:
            filename = asset_url.split('/')[-1]
            local_path = DATA_ROOT / filename
            if not local_path.exists():
                missing_assets.append(filename)
        
        if missing_assets:
            if check_attempt == 0:
                logger.warning(f"Missing data assets: {missing_assets}")
                logger.info("Attempting to download missing assets...")
                download_data_assets()
            else:
                logger.warning(f"Some assets still missing after retry: {missing_assets}")
                logger.info("System will continue and retry these files when needed again")
                break
        else:
            logger.debug("All data assets are available locally")
            break



# ============= OUTPUT FILES =============

# Legacy output files (for backward compatibility)
HIGH_JSON_FILE = BICEP_ROOT / 'high.json'
STATED_JSON_FILE = BICEP_ROOT / 'stated.json'

# ============= AZURE BLOB CONFIGURATION =============

# Azure blob names for database mode
BAU_BUILDING_BLOB_NAME = 'scout-outputs/uec_sdshr_gcam_AEO2023Ref.json'
HIGH_BUILDING_BLOB_NAME = 'scout-outputs/uec_sdshr_gcam_alt-High.json'

# ============= DIRECTORY CREATION =============

def ensure_directories():
    """Create necessary directories if they don't exist."""
    directories = [
        RAW_INPUTS_PATH,
        PARSED_INPUTS_PATH,
        REQUIRED_INPUT_PATH
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

# ============= PATH VALIDATION =============

def validate_required_files():
    """
    Validate that required input files exist.
    Returns list of missing files.
    """
    required_files = [
        COST_FACTOR_FILE,
    ]
    
    missing_files = []
    for file_path in required_files:
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    return missing_files

def validate_data_files():
    """
    Validate that data input files exist for processing.
    Returns list of missing files.
    """
    data_files = [
        SCOUT_BAU_FILE,
        SCOUT_HIGH_FILE,
        EV_PROJECTIONS_FILE,
        PV_PROJECTIONS_FILE,
        HIERARCHY_FILE,
    ]
    
    missing_files = []
    for file_path in data_files:
        if not file_path.exists():
            missing_files.append(str(file_path))
    
    return missing_files

# ============= HELPER FUNCTIONS =============

def get_parsed_output_path(filename):
    """Get full path for a parsed output file."""
    return PARSED_INPUTS_PATH / filename

def get_raw_input_path(filename):
    """Get full path for a raw input file."""
    return RAW_INPUTS_PATH / filename

# Initialize directories on import
ensure_directories()

# ============= CONFIGURATION SUMMARY =============

if __name__ == "__main__":
    print("BICEP Pipeline Configuration")
    print("=" * 40)
    print(f"BICEP Root: {BICEP_ROOT}")
    print(f"Data Root: {DATA_ROOT}")
    print(f"Data Location: {DATA_LOCATION}")
    print(f"Raw Inputs: {RAW_INPUTS_PATH}")
    print(f"Parsed Inputs: {PARSED_INPUTS_PATH}")
    print(f"Required Inputs: {REQUIRED_INPUT_PATH}")
    print()
    
    missing_required = validate_required_files()
    if missing_required:
        print("Warning - Missing Required Files:")
        for file in missing_required:
            print(f"  - {file}")
    else:
        print("Success: All required files present")
    
    missing_data = validate_data_files()
    if missing_data:
        print("\nWarning - Missing Data Files:")
        for file in missing_data:
            print(f"  - {file}")
    else:
        print("Success: All data files present")
