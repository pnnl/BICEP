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

## What's Next?

After working through the basic analysis notebook, you can:
- Explore the BICEP [API Reference](../api-reference) for more advanced usage
- Review the [Methodology](../methodology) to understand how costs are calculated
- Modify the notebook to analyze different scenarios or states