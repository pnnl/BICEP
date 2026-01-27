---
layout: default
title: Scenario Comparison
parent: Examples
nav_order: 3
---

# Scenario Comparison: BAU vs High Demand Growth

This page compares infrastructure upgrade costs across two energy scenarios.

## Overview

BICEP analyzes two contrasting scenarios that represent different levels of technology adoption:

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

## Cost Implications

The choice of scenario dramatically affects infrastructure upgrade requirements.

### Why Costs Differ

| Factor | BAU | High |
|--------|-----|------|
| EV Adoption (2050) | ~20% | ~60% |
| Heat Pump Adoption (2050) | ~15% | ~70% |
| Solar PV Adoption (2050) | ~10% | ~40% |
| **Infrastructure Need** | Low | High |

Higher technology adoption in the High scenario requires more buildings to upgrade their electrical infrastructure.

## Analysis Results

The interactive notebook provides detailed comparisons including:

- **Total cost comparison** - National and state-level estimates
- **Cost by building type** - Residential vs commercial differences
- **Temporal trends** - How costs change over time
- **Geographic variation** - Top states by upgrade costs
- **Capacity requirements** - Electrical load impacts by technology

**[Open the Scenario Comparison Notebook](notebooks/scenario-comparison.ipynb)**

## Key Questions Addressed

### 1. How Much More Does High Scenario Cost?

The High scenario typically requires 50-200% higher infrastructure investment depending on the region.

Example comparison:
```python
from bicep.analysis import BicepResults

bau = BicepResults(scenario='bau')
high = BicepResults(scenario='high')

bau_cost = bau.aggregated['cost'].sum()
high_cost = high.aggregated['cost'].sum()
increase = ((high_cost - bau_cost) / bau_cost) * 100

print(f"Cost increase: {increase:.1f}%")
```

### 2. Which States Face the Largest Costs?

Large population states (CA, TX, NY, FL) face the largest absolute costs, but some smaller states may have higher per-capita costs.

### 3. When Do Costs Peak?

Costs typically peak between 2035-2045 as technologies are rapidly deployed.

### 4. Which Technologies Drive Costs?

- **Heat Pumps**: Often the largest cost driver due to universal applicability
- **EVs**: Significant but concentrated in compatible buildings
- **Solar PV**: Lower direct infrastructure costs but affects net load
- **HPWHs**: Moderate impact across residential sector

### 5. How Do Residential and Commercial Compare?

Residential buildings typically represent 60-80% of upgrade costs due to the larger housing stock, but commercial buildings may have higher per-building costs.

## Using Scenario Results

### Filtering by State

```python
from bicep.analysis import BicepResults

# Analyze specific state
results = BicepResults(scenario='high', target_states='CA')
ca_costs = results.aggregated['cost'].sum()

# Analyze region (multiple states)
results = BicepResults(
    scenario='high',
    target_states=['CA', 'OR', 'WA']
)
west_coast_costs = results.aggregated['cost'].sum()
```

### Comparing Temporal Trends

```python
# Get costs by year
yearly_costs = results.residential.groupby('year')['cost'].sum()

# Identify peak period
peak_year = yearly_costs.idxmax()
peak_cost = yearly_costs.max()

print(f"Peak costs in {peak_year}: ${peak_cost:,.0f}")

# Calculate cumulative costs
cumulative = yearly_costs.cumsum()
```

### Technology-Specific Analysis

```python
# Identify buildings needing EV charging infrastructure
ev_upgrades = results.residential[
    results.residential['ev_req_capacity_amp'] > 
    results.residential['spare_capacity_amp']
]

ev_upgrade_cost = ev_upgrades['cost'].sum()
print(f"EV infrastructure costs: ${ev_upgrade_cost:,.0f}")
```

## Scenario Planning Implications

### Infrastructure Planning

Different scenarios inform different planning horizons:

- **BAU**: Plan for modest upgrades, focus on system efficiency
- **High**: Significant transformer and conductor upgrades needed, new substations may be required

### Investment Strategy

- **Early period (2020-2030)**: Similar costs in both scenarios, use for foundational work
- **Mid period (2030-2040)**: Divergence increases, scenario choice matters greatly
- **Late period (2040-2050)**: High scenario dominates, BAU declines

### Geographic Focus

1. **Large Population Centers**: CA, TX, NY, FL - largest absolute investments
2. **High Adoption Regions**: Northeast, West Coast - likely higher technology adoption
3. **Industrial Areas**: May require specialized upgrades for manufacturing technology adoption

## Advanced Analysis

### Sensitivity Testing

Compare results with different economic assumptions:

```python
# Scenario with different discount rates
low_discount = BicepResults(scenario='high', discount_rate=0.01)
high_discount = BicepResults(scenario='high', discount_rate=0.05)

# Compare present value vs annualized
pv_costs = BicepResults(scenario='high', annualized=False)
annualized = BicepResults(scenario='high', annualized=True)
```

### State-by-State Breakdown

```python
# Compare all states
from bicep.analysis import BicepMultiStateResults

national = BicepMultiStateResults(scenario='high', target_states='all')
state_summary = national.all_states_results

# Identify states with highest per-capita costs
state_summary['per_capita_cost'] = (
    state_summary['total_cost'] / state_summary['population']
)
```

## Next Steps

- **[Data Requirements](data-requirements.html)** - Understand technology adoption assumptions
- **[Custom Distributions](custom-distributions.html)** - Explore cost uncertainty and variation
- **[API Reference](../api-reference.html)** - Access all analysis methods
