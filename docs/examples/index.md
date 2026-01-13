---
layout: default
title: Examples
nav_order: 4
has_children: false
permalink: /examples/
---

# BICEP Examples

This section provides hands-on examples demonstrating how to use BICEP for electrical infrastructure analysis.

## Interactive Jupyter Notebooks

We provide several Jupyter notebooks that demonstrate different aspects of BICEP:

1. **[basic-analysis.ipynb](notebooks/basic-analysis.ipynb)** - Basic analysis workflow
   - [View on GitHub (docs branch)](https://github.com/pnnl/BICEP/blob/docs/docs/examples/notebooks/basic-analysis.ipynb){: target="_blank" }
   - Setting up BICEP analysis
   - Loading pre-computed scenario results
   - Calculating and visualizing costs
   - Key cost drivers

2. **[data-requirements.ipynb](notebooks/data-requirements.ipynb)** - Understanding input data
   - [View on GitHub (docs branch)](https://github.com/pnnl/BICEP/blob/docs/docs/examples/notebooks/data-requirements.ipynb){: target="_blank" }
   - Input data structure and requirements
   - Available technologies and projections
   - Technology forecasts across scenarios
   - Custom forecast integration

3. **[scenario-comparison.ipynb](notebooks/scenario-comparison.ipynb)** - Comparing scenarios
   - [View on GitHub (docs branch)](https://github.com/pnnl/BICEP/blob/docs/docs/examples/notebooks/scenario-comparison.ipynb){: target="_blank" }
   - BAU vs High load growth scenarios
   - Scenario comparison plots and analysis
   - Cost differences and implications

4. **[custom-distributions.ipynb](notebooks/custom-distributions.ipynb)** - Advanced customization
   - [View on GitHub (docs branch)](https://github.com/pnnl/BICEP/blob/docs/docs/examples/notebooks/custom-distributions.ipynb){: target="_blank" }
   - Using custom cost distributions
   - Distribution classes and parameters
   - Sensitivity analysis

### Running the Notebooks

#### Prerequisites

1. **Conda Environment**: Set up the BICEP conda environment (see [Getting Started](../getting-started.html))
2. **Data Files**: Notebooks use the local SQLite database at `data/bicep.x-stock.db`
3. **Jupyter Kernel**: Select the `bicep-env` kernel in VS Code or JupyterLab

#### Launch Instructions

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

## Using BICEP Methods

For detailed API documentation and method references, see the [API Reference](../api-reference.html).

### Quick Start

The simplest way to use BICEP:

```python
from bicep.analysis import BicepResults

# Create results for a scenario
results = BicepResults(scenario='bau')  # or 'high'

# View total costs
print(f"Total infrastructure cost: ${results.total_cost:,.0f}")

# Plot capacity requirements by technology
results.plot_drivers(residential=1)
```

### Next Steps

- [Data Requirements Example](data-requirements.html) - Understanding input data
- [Scenario Comparison](scenario-comparison.html) - Comparing BAU vs High scenarios  
- [Custom Distributions](custom-distributions.html) - Advanced customization
- [API Reference](../api-reference.html) - Complete technical documentation