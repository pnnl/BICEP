---
layout: default
title: Data Requirements
parent: Examples
nav_order: 2
---

# Data Requirements: Understanding BICEP Input Data

This page explains the input data requirements for BICEP and how to use custom technology forecasts.

## Overview

BICEP uses several types of input data to estimate infrastructure upgrade requirements (see the [Methdology](../methodology.html) page for more details):

### 1. Building Stock Data

BICEP analyzes building stocks using data from ComStock and ResStock (xStock) datasets:

- **Building characteristics**: Type (residential/commercial), size, age, location
- **Electrical loads**: Peak electrical loads from energy simulations
- **Existing infrastructure**: Estimated electrical panel capacity using NEC standards

This data is pre-loaded and available in BICEP's built-in database.

### 2. Technology Adoption Forecasts

**Note:** The datasets and forecasts used in this documentation are for illustrative purposes only. The BICEP tool is intended to be run on any arbitrary technology time series projection. To use your own forecasts, you only need base and end years along with the stock count at those times.

BICEP models adoption of four key technologies:

| Technology | End Use | Data Source |
|------------|---------|-------------|
| **Heat Pumps (HP)** | Space heating/cooling | Scout projections |
| **Heat Pump Water Heaters (HPWH)** | Water heating | Scout projections |
| **Solar Photovoltaic (PV)** | Electricity generation | ReEDS projections |
| **Electric Vehicles (EV)** | Transportation | TEMPO projections |

**Available Scenarios:**
- **BAU (Business As Usual)**: Conservative adoption rates
- **High**: High demand growth scenario with higher adoption rates

The differences between scenarios are substantial, particularly for heating technologies. The High scenario assumes 2-3x higher adoption rates by 2050.

### 3. Cost Data

BICEP uses probability distributions for infrastructure upgrade costs:

- Panel upgrade costs (residential and commercial)
- Service line upgrades
- Transformer upgrades
- Material and labor cost variations

See the [Custom Distributions](custom-distributions.html) page for details on cost modeling.

## Using the Data Requirements Notebook

The interactive notebook demonstrates:

- Available technologies and their electrical requirements
- Technology adoption rates across scenarios
- How adoption rates differ over time
- How to integrate custom forecasts

**[Open the Data Requirements Notebook](notebooks/data-requirements.ipynb)**

## Integrating Custom Forecasts

If you have your own technology adoption forecasts, you can integrate them into BICEP analysis.

### Prerequisites

Your forecasts should be organized with:

```
state (str): State abbreviation (e.g., 'CA', 'TX')
year (int): Year of the forecast
ev_adoption_rate (float): Fraction of buildings adopting EVs (0-1)
hp_adoption_rate (float): Fraction adopting heat pumps
hpwh_adoption_rate (float): Fraction adopting heat pump water heaters
pv_adoption_rate (float): Fraction adopting solar PV
```

### Integration Steps

#### Step 1: Load Your Forecasts

```python
import pandas as pd
from bicep.tech_adoption import TechnologyAdoption

# Load your custom adoption forecasts
custom_forecasts = pd.read_csv('my_forecasts.csv')

# Ensure required columns exist
required_cols = ['state', 'year', 'ev_adoption_rate', 'hp_adoption_rate', 
                 'hpwh_adoption_rate', 'pv_adoption_rate']
assert all(col in custom_forecasts.columns for col in required_cols)
```

#### Step 2: Prepare BICEP Building Data

```python
# Initialize BICEP with default forecasts
tech = TechnologyAdoption(scenario='bau')
tech.calculate_adoptions()

# Get the residential data
buildings = tech.residential.copy()
```

#### Step 3: Merge Custom Forecasts

```python
# Merge custom forecasts with building data
buildings = buildings.merge(
    custom_forecasts,
    on=['state', 'year'],
    how='left'
)

# Replace adoption columns with custom values
adoption_cols = {
    'ev_adoption_rate': 'ev_adopted',
    'hp_adoption_rate': 'hp_adopted',
    'hpwh_adoption_rate': 'hpwh_adopted',
    'pv_adoption_rate': 'pv_adopted'
}

for custom_col, bicep_col in adoption_cols.items():
    if custom_col in buildings.columns:
        # For adoption columns, values represent binary (adopted or not)
        buildings[bicep_col] = buildings[custom_col]
```

#### Step 4: Run Analysis with Custom Data

```python
from bicep.upgrades import UpgradeEstimator

# Create upgrade estimator with custom adoption data
estimator = UpgradeEstimator(scenario='bau')

# Replace the residential data with your custom version
estimator.residential = buildings

# Calculate costs using your custom forecasts
estimator.calculate_costs()

# Access results
total_cost = estimator.total_cost
print(f"Total infrastructure cost with custom forecasts: ${total_cost:,.0f}")
```

## Scenario Comparison

To understand how technology adoption differences impact costs, compare BAU and High scenarios:

```python
from bicep.analysis import BicepResults

# Compare scenarios
scenarios = {}
for scenario in ['bau', 'high']:
    results = BicepResults(scenario=scenario, aggregation_level='state')
    scenarios[scenario] = results

# Print comparison
print("Scenario Comparison (2050 Costs):")
for scenario_name, results in scenarios.items():
    total = results.aggregated['cost'].sum()
    print(f"{scenario_name.upper()}: ${total:,.0f}")
```

## Data Validation

Before running BICEP with custom forecasts, validate your data:

```python
def validate_custom_forecasts(df):
    """Check that custom forecasts meet BICEP requirements"""
    
    # Check required columns
    required = ['state', 'year', 'ev_adoption_rate', 'hp_adoption_rate',
                'hpwh_adoption_rate', 'pv_adoption_rate']
    assert all(col in df.columns for col in required), "Missing required columns"
    
    # Check adoption rates are between 0 and 1
    adoption_cols = [col for col in df.columns if 'adoption_rate' in col]
    for col in adoption_cols:
        assert (df[col] >= 0).all() and (df[col] <= 1).all(), \
            f"{col} values must be between 0 and 1"
    
    # Check years are valid
    assert (df['year'] >= 2020).all() and (df['year'] <= 2050).all(), \
        "Years must be between 2020 and 2050"
    
    # Check states are valid US abbreviations
    valid_states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
                    'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
                    'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
                    'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
                    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    assert df['state'].isin(valid_states).all(), "Invalid state abbreviations"
    
    print("✓ Forecasts validation passed!")

# Use the validation function
validate_custom_forecasts(custom_forecasts)
```

## Next Steps

- **[Scenario Comparison](scenario-comparison.html)** - Compare BAU and High scenarios
- **[Custom Distributions](custom-distributions.html)** - Learn about cost distributions
- **[API Reference](../api-reference.html)** - Complete technical documentation
