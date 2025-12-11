---
layout: default
title: Examples
nav_order: 4
has_children: false
permalink: /examples/
---

# BICEP Examples

This section provides hands-on examples demonstrating how to use BICEP for electrical infrastructure analysis.

## Interactive Jupyter Notebook

The best way to get started with BICEP is through our interactive tutorial notebook:

**[basic-analysis.ipynb](https://github.com/pnnl/BICEP/blob/docs/examples/notebooks/basic-analysis.ipynb)**

This notebook demonstrates:
- Setting up the BICEP Python path
- Importing BICEP analysis modules
- Loading pre-computed scenario results (BAU - Business As Usual)
- Calculating total infrastructure upgrade costs
- Visualizing cost drivers (EVs, heat pumps, solar PV, water heaters)

### Running the Notebook

#### Prerequisites

1. **Conda Environment**: Activate the BICEP conda environment:
   ```bash
   conda activate bicep-env
   ```

2. **Data Files**: The notebook uses the local SQLite database at `data/bicep.x-stock.db`. This database is automatically downloaded when you run BICEP for the first time.

3. **Jupyter Kernel**: Make sure your Jupyter kernel is using the `bicep-env` environment. In VS Code or JupyterLab, select the kernel that matches `bicep-env`.

#### Launch the Notebook

**Option 1: VS Code (Recommended)**
- Open the notebook file in VS Code
- Select the `bicep-env` kernel when prompted
- Run cells using Shift+Enter

**Option 2: JupyterLab**
```bash
jupyter lab examples/notebooks/basic-analysis.ipynb
```

**Option 3: Jupyter Notebook**
```bash
jupyter notebook examples/notebooks/basic-analysis.ipynb
```

#### How It Works

The notebook automatically adds the BICEP project to your Python path, so you don't need to install BICEP as a package. The first code cell handles this setup:

```python
import sys
from pathlib import Path

# Get the BICEP root directory (two levels up from this notebook)
bicep_root = Path.cwd().parent.parent
if str(bicep_root) not in sys.path:
    sys.path.insert(0, str(bicep_root))
```

After running this cell, you can import BICEP modules normally.

### Expected Outputs

When you run all cells, you should see:
- **Total cost**: A formatted number showing total infrastructure upgrade costs (in dollars)
- **Interactive plot**: A Plotly visualization breaking down costs by driver type over time

## Using BICEP Methods

BICEP provides several Python modules for analyzing electrical infrastructure upgrade requirements and costs for decarbonization scenarios. Here's a guide to the main classes and methods.

### Core Classes

BICEP uses an inheritance hierarchy with three main classes:

1. **`CapacityEstimate`** - Base class for electrical capacity calculations
2. **`TechnologyAdoption`** - Extends capacity estimates with technology adoption forecasting
3. **`UpgradeEstimator`** - Extends technology adoption with cost analysis
4. **`BicepResults`** - Main analysis class with visualization methods

### BicepResults - Main Analysis Class

The `BicepResults` class is your primary interface for analyzing infrastructure costs. It inherits all functionality from the other classes.

#### Initialization

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

#### Key Attributes

After initialization, `BicepResults` automatically calculates costs and provides these datasets:

```python
# Access residential and commercial datasets
results.residential  # DataFrame with residential building analysis
results.commercial   # DataFrame with commercial building analysis
results.buildings    # Combined residential + commercial data

# Aggregated results
results.aggregated   # Costs aggregated by state/national level
```

#### Visualization Methods

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

#### Analysis Methods

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

## What's Next?

After working through the basic analysis notebook, you can:
- Explore the BICEP [API Reference](../api-reference) for complete technical documentation
- Review the [Methodology](../methodology) to understand how costs are calculated
- Modify the notebook to analyze different scenarios or states
- Create custom analyses using the methods documented above