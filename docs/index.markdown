---
layout: default
title: Home
nav_order: 1
description: "BICEP User Guide - Building Infrastructure Cost Estimation Program"
permalink: /
---

# BICEP User Guide
{: .fs-9 }

Building Infrastructure Cost Estimation Program
{: .fs-6 .fw-300 }

[Get started now](#quick-start){: .get-started-btn }

# BICEP User Guide

Building Infrastructure Cost Estimation Program (BICEP) is a probabilistic model that provides granular estimation of existing electrical capacity and required additional capacity for various technologies in future energy scenarios.

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

- [Getting Started](getting-started.html) - Installation and first steps
- [Data Requirements](examples/data-requirements.html) - Input data formats and sources  
- [Methodology](methodology.html) - Model approach and validation
- [Examples](examples/index.html) - Workflow tutorials and use cases
- [API Reference](api-reference.html) - Key classes and functions

## Citation

If you use BICEP in your research, please cite our IEEE paper:
> Yoder, T., Van Dyke, I., Mott, A., & Esaki-Kua, L. (2025). "Estimating Behind-the-Meter Infrastructure Costs for Electrification Progression." *IEEE Power and Energy Society General Meeting 2025*.
