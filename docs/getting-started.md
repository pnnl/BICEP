---
layout: default
title: Getting Started
---

# Getting Started with BICEP

## Installation

### Prerequisites
- Python 3.11+
- Access to xStock building energy model outputs (optional for custom analysis)

### Install from GitHub
```bash
git clone https://github.com/yourusername/your-repo.git
cd your-repo
pip install -r requirements.txt
pip install .
```

## Basic Workflow

BICEP analysis typically follows this pattern:

1. **Capacity Estimation** - Estimate existing electrical capacity
2. **Technology Adoption** - Model future technology adoption scenarios  
3. **Upgrade Analysis** - Calculate required infrastructure upgrades
4. **Results Analysis** - Visualize and summarize costs

### Simple Example

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

- [Data Requirements](data-requirements.md) - Understanding input data
- [Basic Analysis Example](examples/basic-analysis.md) - Detailed walkthrough
- [Scenario Comparison](examples/scenario-comparison.md) - Comparing BAU vs High scenarios