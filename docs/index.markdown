---
layout: default
title: Home
nav_order: 1
description: "BICEP User Guide - Behind-the-Meter Infrastructure Costs for Electrification Progression"
permalink: /
---

# BICEP User Guide
{: .fs-9 }

Behind-the-Meter Infrastructure Costs for Electrification Progression
{: .fs-6 .fw-300 }

[Get started now](#quick-start){: .get-started-btn }

# BICEP User Guide

Behind-the-Meter Infrastructure Costs for Electrification Progression (BICEP) is a probabilistic model that provides granular estimation of existing electrical capacity and required additional capacity for various technologies in future energy scenarios.

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
> T. Yoder, I. Van Dyke, A. Mott and L. Esaki-Kua, "Estimating Behind-the-Meter Infrastructure Costs for Electrification Progression," 2025 IEEE Power & Energy Society General Meeting (PESGM), Austin, TX, USA, 2025, pp. 1-5, doi: 10.1109/PESGM52009.2025.11225370.
