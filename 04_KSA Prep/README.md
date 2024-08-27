# Project Overview

This project includes scripts for processing and analyzing spatial and observational data. The project is structured with two main scripts: `ksa_decoding.py` and `points_cloning.py`. The scripts are executed using the `main.ipynb` notebook, and the necessary libraries and Python version are specified in the `config.json` file.

## Folder Structure

- `ksa_decoding.py`: Script for relabeling observational data based on specific conditions.
- `points_cloning.py`: Script for generating artificial spatial points around a given set of points.
- `main.ipynb`: Jupyter Notebook that orchestrates the execution of the scripts.
- `config.json`: Configuration file specifying the required libraries and Python version.

## Usage

1. **Install Required Libraries**:
   Ensure all required libraries specified in the `config.json` file are installed in your environment.

2. **Run the Notebook**:
   Open and run all the code cells in `main.ipynb` to execute the scripts and process the data.

## Script Descriptions

### `ksa_decoding.py`
This script contains functions for relabeling observational data based on predefined conditions:

- **conditions**: A dictionary mapping observation values to class labels. Example: `{1: 'V1', 2: 'V2', 3: 'G'}`.

- **relabel_data(sub_data, id_col, nth_col, obs_col)**:
  - `sub_data`: The subset of the DataFrame that needs relabeling.
  - `id_col`: The column name containing unique identifiers for each observation sequence.
  - `nth_col`: The column name representing the order of observations within each sequence.
  - `obs_col`: The column name containing the observation values to be relabeled.

- **process_batch(batch, id_col, nth_col, obs_col)**:
  - `batch`: A batch of data (a subset of the full DataFrame) to be relabeled.
  - `id_col`: The column name containing unique identifiers for each observation sequence.
  - `nth_col`: The column name representing the order of observations within each sequence.
  - `obs_col`: The column name containing the observation values to be relabeled.

- **batch_data(data, id_col, batch_size)**:
  - `data`: The full DataFrame containing all the observation data.
  - `id_col`: The column name containing unique identifiers for each observation sequence.
  - `batch_size`: The number of unique IDs to include in each batch.

- **parallel_relabeling(data, id_col='id_x', nth_col='nth', obs_col='obs', batch_size=1000)**:
  - `data`: The full DataFrame containing all the observation data.
  - `id_col`: The column name containing unique identifiers for each observation sequence.
  - `nth_col`: The column name representing the order of observations within each sequence.
  - `obs_col`: The column name containing the observation values to be relabeled.
  - `batch_size`: The number of unique IDs to include in each batch, used for parallel processing.

### `points_cloning.py`
This script provides a function to generate artificial spatial points in a grid pattern around input points:

- **generate_artificial_points(gdf_points, columns, a=1, distance_meters=100)**:
  - `gdf_points`: A GeoDataFrame containing the original points with their coordinates (x, y).
  - `columns`: A list of column names from `gdf_points` that should be retained and copied to the new artificial points.
  - `a`: The "radius" of the grid around each point, determining how many points are generated. For example, `a=1` generates a 3x3 grid.
  - `distance_meters`: The distance in meters between the original point and each new point in the grid.

## Contributor

Nasiya Alifah Utami
