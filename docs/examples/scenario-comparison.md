---
layout: default
title: Scenario Comparison
parent: Examples
nav_order: 3
---

# Scenario Comparison: BAU vs High Demand Growth

This page shows how you can use BICEP to compare BTM infrastructure upgrade costs between two energy scenarios.

## Overview

BICEP can compare two energy scenarios that represent different levels of technology adoption:

### Business As Usual (BAU)
- Reflects current policies and baseline technology adoption trends
- Lower overall technology adoption rates
- Lower electrical infrastructure upgrade requirements
- Baseline for comparison

### High
- Assumes higher technology adoption and increased demand
- Significantly higher technology adoption rates
- Greater electrical infrastructure demands
- Represents a high-demand scenario

## Analysis Capabilities

The interactive notebook provides detailed comparisons including:

- **Total cost comparison** - National and state-level estimates
- **Cost by building type** - Residential vs commercial differences
- **Temporal trends** - How costs change over time
- **Geographic variation** - Top states by upgrade costs
- **Capacity requirements** - Electrical load impacts by technology

**[Open the Scenario Comparison Notebook](notebooks/scenario-comparison.ipynb)**

## How to Compare Scenarios

### Total Cost Comparison

```python
from bicep.analysis import BicepResults

# Run both scenarios
bau = BicepResults(scenario='bau', mode='local')
high = BicepResults(scenario='high', mode='local')

# Compare total costs
bau_cost = bau.aggregated['cost'].sum()
high_cost = high.aggregated['cost'].sum()
increase = ((high_cost - bau_cost) / bau_cost) * 100

print(f"BAU total cost: ${bau_cost:,.0f}")
print(f"High total cost: ${high_cost:,.0f}")
print(f"Cost increase: {increase:.1f}%")
```

### Cost by Building Type

```python
# Compare residential vs commercial costs across scenarios
for name, results in [('BAU', bau), ('High', high)]:
    res_cost = results.residential['cost'].sum()
    com_cost = results.commercial['cost'].sum()
    print(f"{name} - Residential: ${res_cost:,.0f}, Commercial: ${com_cost:,.0f}")
```

### Temporal Trends

```python
# Get costs by year for each scenario
for name, results in [('BAU', bau), ('High', high)]:
    yearly_costs = results.residential.groupby('year')['cost'].sum()
    peak_year = yearly_costs.idxmax()
    print(f"{name} peak cost year: {peak_year}")
```

### Geographic Variation

```python
# Compare top states by upgrade cost
for name, results in [('BAU', bau), ('High', high)]:
    state_costs = results.buildings.groupby('state')['cost'].sum()
    top_states = state_costs.nlargest(10)
    print(f"\n{name} - Top 10 states by cost:")
    print(top_states)
```

### Capacity Requirements by Technology

```python
# Compare capacity requirements across scenarios
for name, results in [('BAU', bau), ('High', high)]:
    print(f"\n{name} Capacity Requirements:")
    print(results.requirements_by_tech(residential=1))
```

## Filtering by State

```python
from bicep.analysis import BicepResults

# Analyze a specific state under both scenarios
ca_bau = BicepResults(scenario='bau', mode='local', target_states='CA')
ca_high = BicepResults(scenario='high', mode='local', target_states='CA')

# Compare state-level costs
print(f"CA BAU cost: ${ca_bau.total_cost:,.0f}")
print(f"CA High cost: ${ca_high.total_cost:,.0f}")

# Analyze a region (multiple states)
region = BicepResults(
    scenario='high',
    mode='local',
    target_states=['CA', 'OR', 'WA']
)
```

## Technology-Specific Analysis

```python
# Identify buildings needing EV charging infrastructure
ev_upgrades = results.residential[
    results.residential['ev_req_capacity_amp'] > 
    results.residential['spare_capacity_amp']
]

ev_upgrade_cost = ev_upgrades['cost'].sum()
print(f"EV infrastructure costs: ${ev_upgrade_cost:,.0f}")
```

## Next Steps

- **[Data Requirements](data-requirements.html)** - Understand technology adoption assumptions
- **[Custom Distributions](custom-distributions.html)** - Explore cost uncertainty and variation
- **[API Reference](../api-reference.html)** - Access all analysis methods
