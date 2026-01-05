---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started with BICEP
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Environment Setup

### Prerequisites

- **Python 3.11+** - Required for BICEP
- **Conda** (recommended) or pip for package management
- **Git** - To clone the BICEP repository
- **Jupyter** - For running interactive notebooks (optional but recommended)

### Step 1: Install Python and Conda

**Option A: Using Anaconda (Recommended)**

1. Download and install [Anaconda](https://www.anaconda.com/download) for your operating system
2. Verify installation:
   ```bash
   conda --version
   python --version
   ```

**Option B: Using Miniconda (Lightweight Alternative)**

1. Download and install [Miniconda](https://docs.conda.io/projects/miniconda/en/latest/) for your operating system
2. Verify installation:
   ```bash
   conda --version
   python --version
   ```

**Option C: Using Python Virtual Environment (pip)**

If you prefer not to use Conda:

1. Ensure Python 3.11+ is installed:
   ```bash
   python --version
   ```

2. Create a virtual environment:
   ```bash
   python -m venv bicep-env
   ```

3. Activate the environment:
   - **macOS/Linux:**
     ```bash
     source bicep-env/bin/activate
     ```
   - **Windows:**
     ```bash
     bicep-env\Scripts\activate
     ```

### Step 2: Create BICEP Environment

**Using Conda (Recommended)**

Create a dedicated conda environment for BICEP:

```bash
# Create environment with Python 3.11
conda create -n bicep-env python=3.11

# Activate the environment
conda activate bicep-env
```

**Install Dependencies with Conda**

```bash
# Install core dependencies
conda install -c conda-forge numpy pandas scipy scikit-learn matplotlib plotly jupyter sqlalchemy

# Install from requirements file (preferred)
conda install --file requirements.txt
```

### Step 3: Install BICEP

**From GitHub**

```bash
# Clone the repository
git clone https://github.com/pnnl/BICEP.git
cd BICEP

# Install BICEP in development mode
pip install -e .

# Or install from requirements file
pip install -r requirements.txt
```

**Verify Installation**

```bash
# Check that BICEP modules can be imported
python -c "import bicep; print('BICEP installed successfully')"

# Check Python environment
python -c "import sys; print(f'Python: {sys.version}'); import numpy; print(f'NumPy: {numpy.__version__}')"
```

### Step 4: Set Up Jupyter (For Notebooks)

**Install Jupyter**

```bash
# Install Jupyter in the bicep-env environment
conda activate bicep-env
conda install jupyter jupyterlab

# Or with pip
pip install jupyter jupyterlab
```

**Configure Jupyter Kernel**

Make the `bicep-env` available as a Jupyter kernel:

```bash
# Install kernel for this environment
conda activate bicep-env
python -m ipykernel install --user --name bicep-env --display-name "Python (bicep-env)"

# Verify kernel installation
jupyter kernelspec list
```

**Launch Jupyter**

```bash
# Activate the environment
conda activate bicep-env

# Launch JupyterLab (recommended)
jupyter lab

# Or launch Jupyter Notebook
jupyter notebook
```

### Step 5: Verify Setup

Run a quick test to ensure everything is working:

```bash
# Activate environment
conda activate bicep-env

# Run Python
python

# In Python shell, try importing BICEP
>>> from bicep.analysis import BicepResults
>>> results = BicepResults(scenario='bau')
>>> print(f"Setup successful! Total cost: ${results.total_cost:,.0f}")
>>> exit()
```

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

## Working with Notebooks

### Running Notebooks in VS Code

1. **Install VS Code Extension** (if not already installed):
   - Open VS Code
   - Go to Extensions (Cmd+Shift+X on macOS)
   - Search for "Jupyter" and install the official Jupyter extension

2. **Open a Notebook**:
   - Open any `.ipynb` file in VS Code
   - Select the `bicep-env` kernel when prompted
   - Run cells with Shift+Enter

3. **Create a New Notebook**:
   - Use Command Palette (Cmd+Shift+P on macOS)
   - Type "Jupyter: Create New Blank Notebook"

### Running Notebooks in JupyterLab

```bash
# Activate environment and launch JupyterLab
conda activate bicep-env
jupyter lab examples/notebooks/basic-analysis.ipynb
```

### Running Notebooks in Terminal

For non-interactive execution:

```bash
# Convert notebook to Python script and run
jupyter nbconvert --to script my-notebook.ipynb
python my-notebook.py
```

## Troubleshooting

### Issue: "No module named 'bicep'"

**Solution:**
```bash
# Ensure you're in the correct environment
conda activate bicep-env

# Reinstall BICEP in development mode
cd /path/to/BICEP
pip install -e .
```

### Issue: "Jupyter kernel not found"

**Solution:**
```bash
# Reinstall the kernel
conda activate bicep-env
python -m ipykernel install --user --name bicep-env --display-name "Python (bicep-env)"

# Restart Jupyter/VS Code
```

### Issue: Import errors for dependencies

**Solution:**
```bash
# Ensure all dependencies are installed
conda activate bicep-env
pip install -r requirements.txt

# Or reinstall the environment
conda env remove -n bicep-env
conda create -n bicep-env python=3.11
conda activate bicep-env
pip install -r requirements.txt
pip install -e .
```

### Issue: Database connection errors

**Solution:**
- Ensure `data/bicep.x-stock.db` exists in your BICEP directory
- Check file permissions: `ls -la data/bicep.x-stock.db`
- Verify path is correct in your analysis code

## Next Steps

After setting up your environment:

1. **Run the Examples** - Follow the interactive notebooks in [Examples](examples/)
2. **Read the API Reference** - Explore [API Reference](api-reference.md) for detailed method documentation
3. **Understand the Methodology** - See [Methodology](methodology.md) for technical details
4. **Customize Your Analysis** - Use the methods documented in [API Reference](api-reference.md) to create custom analyses