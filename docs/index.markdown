---
layout: default
title: Home
---


# BICEP User Guide

Behind-the-meter Infrastructure Costs for Electrification Progression (BICEP) is a probabilistic model that provides granular estimation of existing electrical capacity and required additional capacity for electrification technologies in future energy scenarios.

## Quick Start

```python
from bicep.analysis import BicepResults

# Run a basic analysis
results = BicepResults(scenario='bau')
results.plot_drivers(residential=1)
```

## Key Features

- **Probabilistic modeling** of electrical infrastructure capacity
- **Multi-sector analysis** (residential, commercial, transportation)
- **Technology adoption forecasting** (heat pumps, EVs, solar PV)
- **Cost estimation** with uncertainty quantification

## Documentation

- [Getting Started](getting-started.md) - Installation and first steps
- [Data Requirements](data-requirements.md) - Input data formats and sources  
- [Methodology](methodology.md) - Model approach and validation
- [Examples](examples/) - Workflow tutorials and use cases
- [API Reference](api-reference.md) - Key classes and functions

## Citation

If you use BICEP in your research, please cite our IEEE paper:
> Yoder, T., Van Dyke, I., Mott, A., & Esaki-Kua, L. (2025). "Estimating Behind-the-Meter Infrastructure Costs for Electrification Progression." *IEEE Power and Energy Society General Meeting 2025*.
