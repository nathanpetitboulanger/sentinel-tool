# Sat-Sentinel Project

## Objective
Create a tool to retrieve Sentinel satellite time series for agricultural parcels using the AWS/STAC/stackstac/xarray stack.

## Functional Requirements
- **Inputs:**
    - `input/` directory containing subdirectories named `YYYY-MM-DD_YYYY-MM-DD` (start and end dates).
    - GPKG files within these subdirectories containing parcel geometries.
    - `config.yaml` at the root to specify desired satellite indices (e.g., NDVI, NDWI).
- **Processing:**
    - Use STAC (SpatioTemporal Asset Catalog) on AWS to find Sentinel-2 data.
    - Use `stackstac` and `xarray` to load and process data.
    - Calculate spatial means (x/y dimensions) for each parcel.
    - Accelerate processing and downloading using `dask`.
- **Outputs:**
    - A single Parquet file in an `output/` directory containing the aggregated time series.

## Technical Stack
- **Language:** Python
- **Libraries:** `pystac-client`, `stackstac`, `xarray`, `geopandas`, `dask`, `pandas`, `pyarrow`, `pyyaml`.
- **Infrastructure:** AWS (via Element 84 STAC API or similar).

## Coding Conventions
- **Type Hinting:** All functions MUST be fully type-hinted.
- **Modularity:** Maintain simple, reusable functions.

## Project Structure
- `src/`: Source code.
- `input/`: Input data (date-based folders + GPKG).
- `output/`: Processed Parquet files.
- `config.yaml`: Global configuration.
