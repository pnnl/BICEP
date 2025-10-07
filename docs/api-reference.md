---
layout: default
title: API Reference
---

# BICEP API Reference

This page provides an overview of the main classes and functions in BICEP. For complete implementation details, see the source code.

## Core Classes

### `CapacityEstimate`
**Location**: `bicep.capacity`

Estimates existing electrical capacity and required additional capacity for electrification technologies.

```python
from bicep.capacity import CapacityEstimate

# Initialize with default parameters
cap = CapacityEstimate()

# Calculate all capacity estimates
cap.calculate_capacity()

# Access results
print(f"Buildings analyzed: {len(cap.buildings)}")
```

**Key Methods:**
- `calculate_existing_capacity()` - Estimates current panel capacity using NEC 220.87
- `building_req_capacity()` - Calculates HP and HPWH capacity requirements
- `pv_req_capacity()` - Estimates PV system capacity requirements  
- `ev_req_capacity()` - Calculates EV charger capacity requirements

**Key Parameters:**
- `residential_voltage` (240V) - Assumed residential service voltage
- `commercial_voltage` (480V) - Assumed light commercial voltage
- `panel_safety_factor` (1.25) - NEC required safety factor

---

### `TechnologyAdoption`
**Location**: `bicep.tech_adoption`

Extends `CapacityEstimate` to model technology adoption scenarios.

```python
from bicep.tech_adoption import TechnologyAdoption

# Create adoption scenario
tech = TechnologyAdoption(scenario='high')  # or 'bau'
tech.calculate_adoptions()

# Check adoption results
ev_adopted = tech.buildings['ev_adopted'].sum()
pv_adopted = tech.buildings['pv_adopted'].sum()
```

**Key Methods:**
- `calculate_adoptions()` - Runs adoption modeling for all technologies
- `_building_adoption(end_use)` - Models HP/HPWH adoption using Scout forecasts
- `_iterative_adoption(tech, tech_project_col)` - Models PV/EV adoption using iterative sampling

**Key Parameters:**
- `scenario` - Either 'bau' or 'high' electrification scenario
- `base_year` (2020) - Starting year for analysis
- `end_year` (2050) - End year for technology adoption

---

### `UpgradeEstimator` 
**Location**: `bicep.upgrades`

Extends `TechnologyAdoption` to estimate upgrade costs.

```python
from bicep.upgrades import UpgradeEstimator

# Calculate upgrade costs
upgrades = UpgradeEstimator(scenario='bau', annualized_costs=True)
upgrades.calculate_costs()

print(f"Total cost: ${upgrades.total_cost:,.0f}")
print(f"Residential: ${upgrades.total_residential_costs:,.0f}")
print(f"Commercial: ${upgrades.total_commercial_costs:,.0f}")
```

**Key Methods:**
- `calculate_costs()` - Runs full cost estimation workflow
- `_required_upgrades()` - Determines which buildings need upgrades
- `_upgrade_costs()` - Assigns costs from probability distributions

**Key Parameters:**
- `annualized_costs` (True) - Return annualized vs. present value costs
- `upgrade_lifespan` (25) - Equipment lifespan for annualization
- `discount_rate` (0.02) - Discount rate for present value calculations
- `nominal_inflation_rate` (0.02) - Inflation rate for cost escalation

---

### `BicepResults`
**Location**: `bicep.analysis`

Extends `UpgradeEstimator` with analysis and visualization methods.

```python
from bicep.analysis import BicepResults

# Create full analysis
results = BicepResults(scenario='high')

# Generate visualizations
results.plot_drivers(residential=1)  # Capacity requirements by tech
results.plot_spare_capacity(residential=1)  # Current spare capacity
results.plot_panel_capacity(residential=1)  # Panel utilization
```

**Key Methods:**
- `plot_drivers(residential, cdf)` - Plot capacity requirements by technology
- `plot_peak_amp_distribution(residential)` - Show peak demand distribution
- `plot_spare_capacity(residential)` - Show available spare capacity
- `requirements_by_tech(residential)` - Summary statistics by technology

## Utility Classes

### Distribution Classes
**Location**: `utils.sampling`

Probability distributions used throughout BICEP:

```python
from utils.sampling import (
    PanelUtilizationDistribution,
    PvSizingDistribution, 
    PanelUpgradeCostDistribution
)

# Panel utilization (empirical data)
panel_util = PanelUtilizationDistribution()
samples = panel_util.constrained_samples(1000, min_value=0.1)

# PV sizing relative to building load
pv_size = PvSizingDistribution() 
pv_samples = pv_size.constrained_samples(1000, min_value=0.01, max_value=1.0)

# Upgrade costs
cost_dist = PanelUpgradeCostDistribution(residential=True)
costs = cost_dist.constrained_samples(1000, min_value=0, max_value=35000)
```

### Database Models
**Location**: `utils.db_models`

SQLAlchemy models for data storage:

- `PeakLoad` - Building peak electrical loads from xStock models
- `StockMeta` - Building characteristics and metadata
- `Technologies` - Technology definitions and capacity requirements
- `AdoptionForecasts` - Technology adoption projections by scenario

## Common Parameters

### Electrical Parameters
- `residential_voltage`: 240V (typical US residential service)
- `commercial_voltage`: 480V (light commercial service)  
- `medium_voltage`: 12,470V (large commercial service)
- `max_light_comm_amp`: 1,000A (threshold for medium voltage)
- `panel_safety_factor`: 1.25 (NEC required safety factor)

### Economic Parameters  
- `discount_rate`: 0.02 (2% real discount rate)
- `nominal_inflation_rate`: 0.02 (2% inflation rate)
- `upgrade_lifespan`: 25 years (equipment lifespan)

### Technology Parameters
- `ev_charger_amp`: 50A (Level 2 EV charger requirement)
- `base_year`: 2020 (analysis starting year)
- `end_year`: 2050 (technology adoption end year)

## Usage Patterns

### Basic Analysis
```python
from bicep.analysis import BicepResults

# Simple scenario analysis
results = BicepResults(scenario='bau')
print(f"Total cost: ${results.total_cost:,.0f}")
```

### Scenario Comparison
```python
bau_results = BicepResults(scenario='bau')
high_results = BicepResults(scenario='high') 

print(f"BAU total: ${bau_results.total_cost:,.0f}")
print(f"High total: ${high_results.total_cost:,.0f}")
print(f"Difference: ${high_results.total_cost - bau_results.total_cost:,.0f}")
```

### Custom Parameters
```python
# Custom economic assumptions
results = BicepResults(
    scenario='high',
    discount_rate=0.03,  # 3% discount rate
    nominal_inflation_rate=0.025,  # 2.5% inflation
    annualized_costs=False  # Present value costs
)
```

## Error Handling

BICEP includes validation for key parameters:

```python
# Scenario validation
try:
    results = BicepResults(scenario='invalid')
except KeyError as e:
    print("Scenario must be 'bau' or 'high'")

# Database connection errors handled automatically with retries
```

For more detailed examples, see the [Examples](examples/) section.