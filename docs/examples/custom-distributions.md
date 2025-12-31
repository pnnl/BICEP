---
layout: default
title: Custom Distributions
parent: Examples
nav_order: 4
---

# Custom Cost Distributions

This page explains how BICEP models cost uncertainty through probability distributions and how to work with custom distributions.

## Overview

BICEP uses probability distributions to model infrastructure upgrade costs because:

1. **Real costs vary** - Building characteristics, regional factors, and market conditions all affect costs
2. **Uncertainty exists** - We don't know exact future prices or site-specific requirements
3. **Risk assessment** - Distributions help quantify best-case, typical-case, and worst-case scenarios

## Cost Uncertainty Sources

| Source | Impact | Example |
|--------|--------|---------|
| **Building size/type** | Large variation | Small single-family vs. large multi-unit |
| **Regional labor costs** | 30-50% variation | Bay Area vs. rural areas |
| **Upgrade complexity** | 2-3x variation | Simple service panel vs. transformer |
| **Supply chain** | Temporal variation | 2020 vs. 2030 pricing |
| **Material availability** | 20-40% variation | Standard vs. specialty components |

## BICEP's Built-in Distributions

BICEP uses three main probability distributions:

### 1. Panel Utilization Distribution

Models how fully existing electrical panels are currently utilized.

**Characteristics:**
- Range: 0.1 - 0.95 (10% to 95% utilization)
- Based on empirical data from building surveys
- Affects how much spare capacity exists

**Usage:**
- Determines which buildings need upgrades
- Directly influences total number of upgrades needed

### 2. PV Sizing Distribution

Represents the relationship between building loads and solar PV system sizes.

**Characteristics:**
- Range: 0.5 - 3.0 times building load
- Log-normal distribution (right-skewed)
- Reflects installer and owner preferences

**Usage:**
- Determines electrical capacity needed for PV
- Affects timing of upgrades

### 3. Panel Upgrade Cost Distribution

Models the cost variation for electrical panel upgrades.

**Characteristics:**
- **Residential**: $2,000 - $50,000
  - Log-normal distribution
  - Mean approximately $8,000-$12,000
  
- **Commercial**: $5,000 - $100,000+
  - Higher variability than residential
  - Mean approximately $15,000-$25,000

**Includes:**
- Equipment costs
- Labor costs
- Permits and inspections
- Site-specific modifications

**Usage:**
```python
from utils.sampling import PanelUpgradeCostDistribution

# Residential costs
res_dist = PanelUpgradeCostDistribution(residential=True)
res_costs = res_dist.constrained_samples(n=1000, min_value=0, max_value=50000)

# Commercial costs
com_dist = PanelUpgradeCostDistribution(residential=False)
com_costs = com_dist.constrained_samples(n=1000, min_value=0, max_value=100000)
```

## Working with Distributions

### Interactive Notebook

The [Custom Distributions notebook](notebooks/custom-distributions.ipynb) demonstrates:

- Sampling from built-in distributions
- Visualizing cost uncertainty
- Analyzing cost variation by technology
- Creating custom distributions
- Assessing distribution impact on total costs

**[Open the Custom Distributions Notebook](notebooks/custom-distributions.ipynb)**

### Code Examples

#### Sampling from Distributions

```python
from utils.sampling import PanelUpgradeCostDistribution

# Create distribution
cost_dist = PanelUpgradeCostDistribution(residential=True)

# Sample 5000 costs
samples = cost_dist.constrained_samples(
    n=5000,
    min_value=1000,
    max_value=40000
)

# Analyze
print(f"Mean: ${samples.mean():,.0f}")
print(f"Median: ${np.median(samples):,.0f}")
print(f"95th percentile: ${np.percentile(samples, 95):,.0f}")
```

#### Understanding the Distribution

```python
import numpy as np
import plotly.graph_objects as go

# Sample from distribution
costs = cost_dist.constrained_samples(n=10000)

# Calculate statistics
stats = {
    'mean': np.mean(costs),
    'median': np.median(costs),
    'std': np.std(costs),
    'q5': np.percentile(costs, 5),
    'q95': np.percentile(costs, 95)
}

# Visualize
fig = go.Figure()
fig.add_trace(go.Histogram(x=costs, nbinsx=50))
fig.update_layout(
    title='Panel Upgrade Cost Distribution',
    xaxis_title='Cost ($)',
    yaxis_title='Frequency'
)
fig.show()
```

## Creating Custom Distributions

If BICEP's default distributions don't match your region or situation, you can create custom distributions.

### Approach 1: Empirical Distribution (from your data)

Use observed costs from your region or organization.

```python
def create_empirical_distribution(observed_costs):
    """
    Create a distribution from observed cost data.
    
    Args:
        observed_costs: array of actual costs from your data
    
    Returns:
        Function that samples from the empirical distribution
    """
    sorted_costs = np.sort(observed_costs)
    
    def sample(n=1):
        # Random sampling with replacement from observed data
        return np.random.choice(sorted_costs, size=n, replace=True)
    
    return sample

# Example usage
my_costs = np.array([5000, 7500, 8000, 9000, 12000, 15000, ...])
my_distribution = create_empirical_distribution(my_costs)

# Sample from custom distribution
samples = my_distribution(n=1000)
```

### Approach 2: Parametric Distribution (Log-Normal)

Use a statistical distribution fitted to your data.

```python
from scipy import stats

def create_lognormal_distribution(observed_costs, min_val=0, max_val=np.inf):
    """
    Fit a log-normal distribution to cost data.
    
    Log-normal distributions are ideal for costs because:
    - Always positive (no negative costs)
    - Right-skewed (most common, some expensive outliers)
    - Common in engineering cost estimates
    
    Args:
        observed_costs: array of observed costs
        min_val, max_val: constraints on sampled values
    
    Returns:
        Function that samples from the distribution
    """
    # Fit log-normal parameters to data
    shape, loc, scale = stats.lognorm.fit(observed_costs)
    
    def sample(n=1):
        samples = stats.lognorm.rvs(shape, loc, scale, size=n)
        # Constrain to range
        samples = np.clip(samples, min_val, max_val)
        return samples
    
    return sample

# Example usage
my_distribution = create_lognormal_distribution(
    observed_costs=my_costs,
    min_val=1000,
    max_val=50000
)

samples = my_distribution(n=1000)
```

### Approach 3: Regional Adjustment Factors

Adjust BICEP's default distribution based on regional factors.

```python
def create_regional_distribution(base_distribution, regional_factor):
    """
    Scale costs by regional factor.
    
    Example factors:
    - 0.8: Low cost region (rural, lower labor costs)
    - 1.0: National average (BICEP default)
    - 1.3: High cost region (urban, high labor costs)
    
    Args:
        base_distribution: BICEP's distribution function
        regional_factor: Multiplier for regional variation
    """
    def sample(n=1):
        base_samples = base_distribution.constrained_samples(n=n)
        return base_samples * regional_factor
    
    return sample

# Example: California costs are ~20% higher than national average
from utils.sampling import PanelUpgradeCostDistribution
base_dist = PanelUpgradeCostDistribution(residential=True)
ca_distribution = create_regional_distribution(base_dist, regional_factor=1.2)

ca_costs = ca_distribution(n=1000)
```

## Impact on Results

### Cost Estimate Uncertainty

Different distributions lead to different total cost estimates.

Example ranges:
- **Conservative** (lower distribution): $X
- **Mid-point** (BICEP default): $Y
- **High-end** (upper distribution): $Z

### Sensitivity Analysis

Test how distribution choice affects conclusions:

```python
from bicep.upgrades import UpgradeEstimator

scenarios = {
    'conservative': {'distribution_multiplier': 0.8},
    'mid_point': {'distribution_multiplier': 1.0},
    'high': {'distribution_multiplier': 1.2}
}

for scenario_name, params in scenarios.items():
    estimator = UpgradeEstimator(scenario='high')
    
    # Apply custom distribution adjustment
    # (pseudocode - actual implementation depends on BICEP architecture)
    estimator.cost_multiplier = params['distribution_multiplier']
    estimator.calculate_costs()
    
    total = estimator.total_cost
    print(f"{scenario_name}: ${total:,.0f}")
```

## Best Practices

### When Choosing Distributions:

1. **Use empirical data when available** - Real observed costs are more credible
2. **Validate assumptions** - Check that distribution parameters match your data
3. **Document sources** - Record where distribution parameters come from
4. **Perform sensitivity analysis** - Test high and low scenarios
5. **Regional adjustments** - Account for local cost differences

### Questions to Ask:

- What cost data do I have access to?
- How applicable are BICEP's national distributions to my region?
- What's the range of uncertainty I should plan for?
- How do costs vary by building type?
- Are there temporal variations I should account for?

## Advanced Topics

### Monte Carlo Simulation

Run uncertainty analysis with many sample iterations:

```python
import numpy as np
from bicep.analysis import BicepResults

# Run multiple analyses with different random seeds
n_iterations = 100
total_costs = []

for i in range(n_iterations):
    np.random.seed(i)  # Different distribution sample each iteration
    results = BicepResults(scenario='high')
    total_costs.append(results.total_cost)

# Analyze uncertainty
print(f"Mean: ${np.mean(total_costs):,.0f}")
print(f"5th percentile: ${np.percentile(total_costs, 5):,.0f}")
print(f"95th percentile: ${np.percentile(total_costs, 95):,.0f}")
```

### Distribution Comparison

Formally compare different distributions using statistical tests:

```python
from scipy import stats

# Test if two distributions are significantly different
statistic, p_value = stats.ks_2samp(default_samples, custom_samples)

if p_value < 0.05:
    print("Distributions are significantly different")
else:
    print("Distributions are not significantly different")
```

## Next Steps

- **[Data Requirements](data-requirements.md)** - Understand technology adoption
- **[Scenario Comparison](scenario-comparison.md)** - Compare cost scenarios
- **[API Reference](../api-reference.md)** - Full API documentation
