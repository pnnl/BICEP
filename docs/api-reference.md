---
layout: default
title: API Reference
nav_order: 5
---

# BICEP API Reference

This page provides an overview of the main classes and functions in BICEP. For complete implementation details, see the source code.

**→ [View Full Reference (Utilities & Helper Functions)](#utility-classes)**

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

## BicepResults - Main Analysis Class

The `BicepResults` class is your primary interface for analyzing infrastructure costs. It inherits all functionality from the other classes.

### Initialization

```python
from bicep.analysis import BicepResults

# Create an analysis instance
results = BicepResults(
    aggregation_level='state',  # 'state' or 'national'
    scenario='bau',             # 'bau' or 'high'
    annualized=True,            # True for annualized costs, False for present value
    upgrade_lifespan=25,        # Years for annualization
    base_year=2020,             # Start year for analysis
    end_year=2050,              # End year for projections
    discount_rate=0.02,         # Discount rate (2%)
    nominal_inflation_rate=0.02 # Inflation rate (2%)
)
```

**Key Parameters:**
- `aggregation_level`: Spatial aggregation - `'state'` for state-level analysis or `'national'` for national totals
- `scenario`: Decarbonization scenario - `'bau'` (Business As Usual) or `'high'` (High electrification)
- `annualized`: If `True`, returns annualized costs; if `False`, returns present value
- `discount_rate`: Used to bring future costs to present value
- `nominal_inflation_rate`: Used to escalate costs to future years

### Key Attributes

After initialization, `BicepResults` automatically calculates costs and provides these datasets:

```python
# Access residential and commercial datasets
results.residential  # DataFrame with residential building analysis
results.commercial   # DataFrame with commercial building analysis
results.buildings    # Combined residential + commercial data

# Aggregated results
results.aggregated   # Costs aggregated by state/national level
```

### Visualization Methods

**1. Plot Cost Drivers Over Time**

```python
# Plot residential infrastructure upgrade drivers
results.plot_drivers(residential=1, cdf=True)
```
- `residential`: `1` for residential, `0` for commercial, or any other value for both
- `cdf`: If `True`, shows cumulative distribution; if `False`, shows frequency distribution

This creates an interactive Plotly chart showing how different technologies (EVs, heat pumps, PV, water heaters) drive infrastructure upgrade costs over time.

**2. Plot Peak Amperage Distribution**

```python
# Visualize the distribution of peak electrical loads
results.plot_peak_amp_distribution(residential=1)
```

Shows the distribution of peak amperage across buildings, helping identify load patterns.

**3. Plot Spare Capacity**

```python
# Analyze available spare capacity in existing infrastructure
results.plot_spare_capacity(residential=1)
```

Displays how much spare capacity exists before upgrades are needed.

**4. Plot Panel Capacity Distribution**

```python
# Show distribution of electrical panel sizes
results.plot_panel_capacity(residential=1, log_y=True)
```
- `log_y`: Use logarithmic scale for y-axis

### Analysis Methods

**Get Capacity Requirements by Technology**

```python
# Statistical summary of capacity requirements for each technology
stats = results.requirements_by_tech(residential=1)
print(stats)
```

Returns a DataFrame with descriptive statistics (count, mean, std, min, max, percentiles) for:
- `ev_req_capacity_amp` - EV charging capacity needs
- `pv_req_capacity_amp` - Solar PV capacity needs
- `hp_req_capacity_amp` - Heat pump capacity needs
- `hpwh_req_capacity_amp` - Heat pump water heater capacity needs

### Working with Results Data

The main datasets contain detailed building-level information:

```python
# Access residential results
df = results.residential

# Key columns in the dataset:
# - 'state': State abbreviation
# - 'year': Projection year
# - 'cost': Upgrade cost (annualized or present value)
# - 'ev_req_capacity_amp': Required capacity for EVs (amps)
# - 'pv_req_capacity_amp': Required capacity for PV (amps)
# - 'hp_req_capacity_amp': Required capacity for heat pumps (amps)
# - 'hpwh_req_capacity_amp': Required capacity for water heaters (amps)
# - 'peak_amp': Peak electrical load (amps)
# - 'estimated_capacity_amp': Estimated existing panel capacity (amps)

# Calculate total costs by state and year
total_by_state = df.groupby(['state', 'year'])['cost'].sum()

# Filter for a specific state
california = df[df['state'] == 'CA']

# Analyze costs by technology driver
ev_costs = df[df['ev_req_capacity_amp'] > 0]['cost'].sum()
```

### Advanced Usage: Lower-Level Classes

#### TechnologyAdoption

If you only need technology adoption forecasts without cost analysis:

```python
from bicep.tech_adoption import TechnologyAdoption

tech = TechnologyAdoption(
    scenario='bau',
    base_year=2020,
    end_year=2050
)

# Calculate technology adoption rates
tech.calculate_adoptions()

# Access adoption data
tech.residential  # Residential buildings with adoption data
tech.commercial   # Commercial buildings with adoption data
```

#### CapacityEstimate

For electrical capacity calculations only:

```python
from bicep.capacity import CapacityEstimate

capacity = CapacityEstimate(
    residential_voltage=240,      # Residential voltage (volts)
    commercial_voltage=480,       # Commercial voltage (volts)
    ev_charger_amp=50,           # EV charger amperage
    panel_safety_factor=1.25     # NEC safety factor
)

# Calculate building capacity estimates
capacity.calculate_capacity()
```

### Example Workflow

Here's a complete example analyzing costs for different scenarios:

```python
from bicep.analysis import BicepResults
import pandas as pd

# Compare BAU vs High electrification scenarios
scenarios = {}
for scenario in ['bau', 'high']:
    results = BicepResults(
        scenario=scenario,
        aggregation_level='state',
        annualized=True
    )
    scenarios[scenario] = results

# Compare total costs
for name, results in scenarios.items():
    total_cost = results.aggregated['cost'].sum()
    print(f"{name.upper()} total cost: ${total_cost:,.0f}")

# Visualize each scenario
scenarios['bau'].plot_drivers(residential=1)
scenarios['high'].plot_drivers(residential=1)

# Compare capacity requirements
for name, results in scenarios.items():
    print(f"\n{name.upper()} Capacity Requirements:")
    print(results.requirements_by_tech(residential=1))
```

### Tips and Best Practices

1. **Start with BicepResults**: The `BicepResults` class includes all functionality you'll typically need
2. **Check data availability**: Ensure `data/bicep.x-stock.db` exists before running analyses
3. **Use appropriate aggregation**: State-level provides more detail; national-level is faster for overview
4. **Annualized vs Present Value**: Use annualized costs for budgeting; present value for total investment analysis
5. **Filter by residential/commercial**: Most methods accept a `residential` parameter to focus your analysis
6. **Explore interactively**: Use Jupyter notebooks to iteratively explore results and visualizations

## Running BICEP: Modes and State Selection

BICEP supports flexible execution modes and state targeting. This section shows how to run analyses for different geographic scopes.

### Data Modes

BICEP can run in two data modes:

| Mode | Description | Use Case |
|------|-------------|----------|
| `'local'` | Uses local parquet files in `data/` | Offline analysis, faster execution |
| `'PNNL database'` | Connects to PNNL's remote database | Access to latest data |

### Running in Local Mode

Local mode uses pre-downloaded data files. This is the recommended mode for most users.

```python
from bicep.analysis import BicepResults

# Run analysis using local data files
results = BicepResults(
    scenario='bau',
    mode='local'  # Uses local parquet files
)

print(f"Total cost: ${results.total_cost:,.0f}")
```

### Running for a Single State

To analyze just one state, use the `target_states` parameter with a state abbreviation:

```python
from bicep.analysis import BicepResults

# Analyze only California
ca_results = BicepResults(
    scenario='bau',
    mode='local',
    target_states='CA'
)

print(f"California total cost: ${ca_results.total_cost:,.0f}")
print(f"Residential: ${ca_results.total_residential_costs:,.0f}")
print(f"Commercial: ${ca_results.total_commercial_costs:,.0f}")

# Access California building data
ca_buildings = ca_results.buildings
print(f"Buildings analyzed: {len(ca_buildings):,}")
```

### Running for Multiple States

Pass a list of state abbreviations to analyze multiple states together:

```python
from bicep.analysis import BicepResults

# Analyze West Coast states
west_coast = BicepResults(
    scenario='bau',
    mode='local',
    target_states=['CA', 'OR', 'WA']
)

print(f"West Coast total cost: ${west_coast.total_cost:,.0f}")

# Break down by state
for state in ['CA', 'OR', 'WA']:
    state_data = west_coast.buildings[west_coast.buildings['state'] == state]
    state_cost = state_data['weighted_cost'].sum()
    print(f"  {state}: ${state_cost:,.0f}")
```

### Running for All States (Multi-State Analysis)

For national analysis, use `BicepMultiStateResults` which processes each state individually to avoid competition effects, then combines results:

```python
from bicep.analysis import BicepMultiStateResults

# Run analysis for all 50 states + DC
national = BicepMultiStateResults(
    scenario='bau',
    mode='local',
    target_states='all',      # Analyze all states
    save_results=True         # Save outputs to CSV files
)

# Access national totals
print(f"National total cost: ${national.total_cost:,.0f}")
print(f"Residential: ${national.total_residential_costs:,.0f}")
print(f"Commercial: ${national.total_commercial_costs:,.0f}")

# Access combined data from all states
all_buildings = national.all_states_buildings
all_meta = national.all_states_meta
state_summary = national.all_states_results

print(f"\nTotal buildings analyzed: {len(all_buildings):,}")
print(f"States included: {len(state_summary)}")
```

### Multi-State Analysis with Custom State List

You can also use `BicepMultiStateResults` for a subset of states:

```python
from bicep.analysis import BicepMultiStateResults

# Analyze only specific regions
southeast = BicepMultiStateResults(
    scenario='high',
    mode='local',
    target_states=['FL', 'GA', 'NC', 'SC', 'TN', 'AL'],
    save_results=False  # Don't save to files
)

print(f"Southeast region cost: ${southeast.total_cost:,.0f}")

# View per-state breakdown
print(southeast.all_states_results)
```

### Output Files

When `save_results=True`, BICEP saves three CSV files to `data/parsed_inputs/`:

| File | Description |
|------|-------------|
| `bicep_cost_summary_{scenario}.csv` | Aggregate costs by state and building type |
| `bicep_results_{scenario}_all_states.csv` | Detailed building-level results |
| `bicep_meta_{scenario}_all_states.csv` | Building metadata |

```python
# Files are saved automatically when save_results=True
results = BicepMultiStateResults(
    scenario='bau',
    mode='local',
    save_results=True
)

# Or manually save later
results.save_results()
```

### Quick Reference: State Targeting

| Target | Code Example |
|--------|--------------|
| Single state | `target_states='CA'` |
| Multiple states | `target_states=['CA', 'TX', 'NY']` |
| All states | `target_states='all'` |

### Valid State Abbreviations

BICEP supports all 50 US states plus Washington DC:

```
AL, AK, AZ, AR, CA, CO, CT, DE, DC, FL, GA, HI, ID, IL, IN, IA, KS, KY, LA, ME,
MD, MA, MI, MN, MS, MO, MT, NE, NV, NH, NJ, NM, NY, NC, ND, OH, OK, OR, PA, RI,
SC, SD, TN, TX, UT, VT, VA, WA, WV, WI, WY
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