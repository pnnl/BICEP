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
PV_PROJECTIONS_FILE = RAW_INPUTS_PATH / 'distpvcap_stscen2023_mid_case_utf8.csv'

# Hierarchy and mapping files
HIERARCHY_FILE = RAW_INPUTS_PATH / 'hierarchy.csv'
TECHNOLOGY_MAP_FILE = BICEP_ROOT / 'technology_map.csv'

# Required input files
COST_FACTOR_FILE = REQUIRED_INPUT_PATH / 'cost_factor.csv'

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
