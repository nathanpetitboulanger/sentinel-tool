from pathlib import Path
from typing import Any, Dict, List, Optional, cast
import pickle

import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
import pystac_client
import rioxarray
import stackstac
import xarray as xr
from dask.distributed import Client
from pyproj import CRS


def calculate_indices(ds: xr.Dataset, indices: List[str]) -> xr.Dataset:
    """Calculate satellite indices from dataset."""
    res = {}
    available_bands = ds.band.values.tolist()

    if "NDVI" in indices and "nir" in available_bands and "red" in available_bands:
        nir = ds.sel(band="nir")
        red = ds.sel(band="red")
        denom = nir + red
        res["NDVI"] = xr.where(denom != 0, (nir - red) / denom, np.nan)

    if "NDWI" in indices and "green" in available_bands and "nir" in available_bands:
        green = ds.sel(band="green")
        nir = ds.sel(band="nir")
        denom = green + nir
        res["NDWI"] = xr.where(denom != 0, (green - nir) / denom, np.nan)

    return xr.Dataset(res)


def get_usable_timestamps(
    items: pystac.ItemCollection,
    bbox_utm: List[float],
    gdf_utm: gpd.GeoDataFrame,
    crs: CRS,
    threshold: float = 0.5,
) -> pd.DatetimeIndex:
    """Fetch SCLand return timestamps where the parcel is mostly valid."""
    scl_stack = stackstac.stack(
        items,
        bounds=bbox_utm,  # type: ignore
        assets=["scl"],
        epsg=crs.to_epsg(),
        resolution=20,
        dtype="uint8",  # type: ignore
        fill_value=0,
        chunksize=1024,
    )

    # Valid SCL: 4 (Veg), 5 (Soil), 6 (Water), 7 (Unclassified)
    valid_scl = [4, 5, 6, 7]
    clipped_scl = scl_stack.rio.clip(gdf_utm.geometry, crs)

    is_valid_pixel = clipped_scl.sel(band="scl").isin(valid_scl)
    valid_ratio = is_valid_pixel.mean(dim=["x", "y"]).compute()

    keep_times = valid_ratio.time[valid_ratio > threshold].values
    return pd.to_datetime(keep_times)


def process_single_parcel(
    items: pystac.ItemCollection,
    row: pd.Series,
    bbox_utm: List[float],
    crs: CRS,
    config: Dict[str, Any],
    folder_name: str,
    file_name: str,
) -> Optional[pd.DataFrame]:
    """Process a single parcel: filter by SCL, load bands, and calculate indices."""
    keep_times = get_usable_timestamps(
        items, bbox_utm, gpd.GeoDataFrame([row], crs=crs), crs
    )

    if len(keep_times) == 0:
        print(f"No usable images (SCL) for parcel {row.get('id')}")
        return None

    # Filter items based on selected timestamps
    selected_items = [
        item
        for item in items
        if any(
            np.abs(
                (
                    item.datetime.replace(tzinfo=None)
                    - t.to_pydatetime().replace(tzinfo=None)
                ).total_seconds()
            )
            < 1
            for t in keep_times
        )
    ]

    if not selected_items:
        return None

    bands = ["green", "red", "nir"]
    stack = stackstac.stack(
        selected_items,
        bounds=bbox_utm,  # type: ignore
        assets=bands,
        epsg=crs.to_epsg(),
        resolution=10,
        dtype="float32",  # type: ignore
        fill_value=np.float32(np.nan),  # type:ignore
        chunksize=1024,
    )

    if stack.rio.crs:
        stack = stack.rio.write_crs(stack.rio.crs)

    clipped_bands = stack.rio.clip([row.geometry], crs)
    clipped_bands = cast(xr.Dataset, clipped_bands)

    # Code de debug pour inspecter les données localement
    # On nettoie les coordonnées et attributs STAC qui bloquent la sérialisation Zarr
    debug_ds = clipped_bands.reset_coords(drop=True)
    debug_ds.attrs = {}
    debug_ds.to_zarr("ds.zarr", mode="w")

    mean_bands = clipped_bands.mean(dim=["x", "y"]).compute()
    indices_ds = calculate_indices(mean_bands, config["indices"])

    df = indices_ds.to_dataframe().reset_index()

    # Metadata
    df["parcel_id"] = row.get("id")
    df["source_file"] = file_name
    df["date_range"] = folder_name

    cols = ["time"] + config["indices"] + ["parcel_id", "source_file", "date_range"]
    return df[[c for c in cols if c in df.columns]]


def process_date_folder(
    folder_path: Path, config: Dict[str, Any]
) -> List[pd.DataFrame]:
    """Iterate over GPKG files in a date folder and process parcels."""
    dates = folder_path.name.split("_")
    if len(dates) != 2:
        return []

    start_date, end_date = dates
    all_results = []
    catalog = pystac_client.Client.open(config["stac_api_url"])

    for gpkg_file in folder_path.glob("*.gpkg"):
        gdf = gpd.read_file(gpkg_file)
        if gdf.empty:
            continue

        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)

        utm_crs = gdf.estimate_utm_crs()
        gdf_utm = gdf.to_crs(utm_crs)

        search = catalog.search(
            collections=[config["collection"]],
            bbox=gdf.to_crs("EPSG:4326").total_bounds.tolist(),
            datetime=f"{start_date}/{end_date}",
            query={"eo:cloud_cover": {"lt": 20}},
        )

        items = search.item_collection()
        print(f"Found {len(items)} items for {gpkg_file.name}")
        if len(items) == 0:
            continue

        for idx, row in gdf_utm.iterrows():
            # Ensure an ID exists
            if row.get("id") is None:
                row["id"] = f"{gpkg_file.stem}_{idx}"

            res = process_single_parcel(
                items,
                row,
                gdf_utm.total_bounds.tolist(),
                utm_crs,
                config,
                folder_path.name,
                gpkg_file.name,
            )
            if res is not None:
                all_results.append(res)

    return all_results
