# CLAUDE.md — Sentinel Tool

## Project goal

Tool for processing **Sentinel-2** satellite image time series over agricultural parcels.
For each geographic parcel (`.gpkg`), it computes **spectral indices** (NDVI, NDWI) over a given period, filters clouds pixel by pixel, and produces a time series as output (`.parquet`).

---

## Architecture

Three classes with separate responsibilities + a main entry point:

| Class | File | Role |
|---|---|---|
| `StacFetcher` | `src/stac_fetcher.py` | Queries the AWS STAC API (Element84). Handles retries and splitting of periods > 1 year. |
| `MathEngine` | `src/math_engine.py` | Loads pixels from S3 via `odc.stac`, filters clouds via SCL, computes NDVI/NDWI, produces spatial means. |
| `Orchestrator` | `src/orchestrator.py` | Reads `.gpkg` files, splits into spatial batches (~0.5°), parallelises with `ThreadPoolExecutor`. |
| `main` | `src/main.py` | Initialises Dask, iterates over `input/`, saves output as `.parquet`. |

### Data flow

```
input/
  YYYY-MM-DD_YYYY-MM-DD/
    parcels.gpkg
         ↓
    Spatial batching (0.5° grid)
         ↓
    STAC API → Sentinel-2 images (< 80% cloud cover)
         ↓
    SCL filtering (> 50% clear pixels required)
         ↓
    NDVI/NDWI computation → spatial mean per date
         ↓
output/
  mean/sentinel_time_series.parquet
  raw/<parcel_id>.zarr  (optional, config save_zarr: true)
```

---

## Tech stack

- **Python 3.12+**, managed with `uv`
- **`odc-stac`**: pixel loading from S3 (replaces `stackstac`)
- **`pystac-client`**: STAC API querying
- **`xarray` + `rioxarray`**: raster cube manipulation
- **`geopandas` + `shapely`**: geometry manipulation
- **`dask` + `distributed`**: parallelism and distributed computation
- **`pandas` + `pyarrow`**: Parquet output
- **`zarr`**: optional raw cube saving
- **STAC API**: `https://earth-search.aws.element84.com/v1`, collection `sentinel-2-c1-l2a`

---

## Configuration (`config.yaml`)

```yaml
indices: [NDVI]           # Indices to compute (NDVI, NDWI supported)
stac_api_url: "..."       # STAC catalogue URL
collection: "..."         # Sentinel-2 collection
dask:
  n_workers: 4
  threads_per_worker: 10
save_zarr: true           # Save raw cubes as .zarr
```

---

## Logging

Logging is configured via `src/config.py::setup_logging()`, called at the start of `main()`.

- Logs written to `logs/YYYYMMDD_HHMMSS.log` AND to the console
- Format: `YYYY-MM-DD HH:MM:SS [LEVEL] module: message`
- Each module uses `logging.getLogger(__name__)`
- Levels: INFO (progress), WARNING (missing data/clouds), ERROR (S3/STAC failure), `logger.exception()` in `except` blocks for full stack traces

---

## Code conventions

- **Type hints** required on all functions
- **No `print()`** in `src/` — use `logger.*`
- Prefer editing existing files over creating new ones
- All functions must have a docstring

---

## Directory structure

```
sentinel-tool/
├── src/
│   ├── config.py         # load_config() + setup_logging()
│   ├── main.py           # Entry point
│   ├── orchestrator.py   # Orchestrator: spatial batching + parallelism
│   ├── stac_fetcher.py   # StacFetcher: STAC API with retries
│   └── math_engine.py    # MathEngine: SCL, indices, spatial means
├── input/                # YYYY-MM-DD_YYYY-MM-DD/ folders with .gpkg files
├── output/
│   ├── mean/             # sentinel_time_series.parquet
│   └── raw/              # Per-parcel .zarr cubes (optional)
├── logs/                 # Timestamped log files
├── storage/              # Local GIS data (RPG, etc.)
├── config.yaml
└── pyproject.toml
```
