import pathlib
from loguru import logger

ROOT = pathlib.Path(__file__).parent.parent
DATA_DIR = ROOT / 'data'

DATA_LOCATION = 'LOCAL'
try:
    assert DATA_LOCATION in ('LOCAL', 'PNNL Database')
except AssertionError as error:
    logger.error(f'DATA_LOCATION {DATA_LOCATION} is not valid. Must be in ["LOCAL", "PNNL Database"]')
    logger.error(error)
    raise AssertionError
