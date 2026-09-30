"""
BICEP - Behind-the-Meter Infrastructure Costs for Electrification Progression

Main module for estimating electrical infrastructure upgrade costs
for various energy scenarios.
"""

from bicep.analysis import BicepResults, BicepMultiStateResults
from bicep.capacity import CapacityEstimate
from bicep.tech_adoption import TechnologyAdoption
from bicep.upgrades import UpgradeEstimator

__all__ = [
    'BicepResults',
    'BicepMultiStateResults',
    'CapacityEstimate',
    'TechnologyAdoption',
    'UpgradeEstimator',
]

__version__ = '1.0.0'
