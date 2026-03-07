import pystac_client
import stackstac
import xarray as xr
import numpy as np
import geopandas as gpd
import pandas as pd
from src.config import load_config
from src.processing import SentinelProcessor
from pathlib import Path

def investigate():
    config = load_config()
    processor = SentinelProcessor(config)
    
    # Use the 2024 sample
    folder = Path("input/2024-01-01_2024-12-31")
    gpkg = next(folder.glob("*.gpkg"))
    gdf = gpd.read_file(gpkg)
    utm_crs = gdf.estimate_utm_crs()
    gdf_utm = gdf.to_crs(utm_crs)
    
    catalog = pystac_client.Client.open(config["stac_api_url"])
    search = catalog.search(
        collections=[config["collection"]],
        bbox=gdf.to_crs("EPSG:4326").total_bounds.tolist(),
        datetime="2024-01-01/2024-12-31",
        query={"eo:cloud_cover": {"lt": 20}},
    )
    items = search.item_collection()
    print(f"Items found: {len(items)}")
    
    # Just take 20 items to be fast
    items = items[:20]
    
    stack = stackstac.stack(
        items,
        bounds=gdf_utm.total_bounds.tolist(),
        assets=["green", "red", "nir"],
        epsg=utm_crs.to_epsg(),
        resolution=10,
        dtype="float32",
        fill_value=np.float32(np.nan),
        chunksize=1024,
    ).rio.write_crs(utm_crs)
    
    indices_ds = processor._calculate_indices(stack)
    
    # Check for NDVI > 1
    # Trigger compute on a small portion
    print("Computing NDVI min/max...")
    ndvi_vals = indices_ds.NDVI.compute()
    ndvi_max = ndvi_vals.max().values
    ndvi_min = ndvi_vals.min().values
    print(f"NDVI Min: {ndvi_min}, Max: {ndvi_max}")
    
    # Find points where NDVI > 1
    high_ndvi_mask = (ndvi_vals > 1)
    high_ndvi_count = high_ndvi_mask.sum().values
    print(f"Pixels with NDVI > 1: {high_ndvi_count}")
    
    if high_ndvi_count > 0:
        # Inspect a pixel with high NDVI
        # Find indices of first True
        indices = np.where(high_ndvi_mask.values)
        if len(indices[0]) > 0:
            t_idx, y_idx, x_idx = indices[0][0], indices[1][0], indices[2][0]
            # Get values for this pixel
            pixel_vals = stack.isel(time=t_idx, y=y_idx, x=x_idx).compute()
            print(f"Pixel values at high NDVI: {pixel_vals}")
            print(f"NDVI at that pixel: {ndvi_vals.isel(time=t_idx, y=y_idx, x=x_idx).values}")
            
            # Check bands
            bands = stack.band.values.tolist()
            red_val = pixel_vals.sel(band="red").values
            nir_val = pixel_vals.sel(band="nir").values
            print(f"NIR: {nir_val}, Red: {red_val}")
    
    # Inspect why Zarr fails
    print("\nInspecting dataset for Zarr serialization...")
    for name, var in indices_ds.variables.items():
        print(f"Variable: {name}, dtype: {var.dtype}")
    for name, attr in indices_ds.attrs.items():
        print(f"Attr: {name}, value: {attr}")

    processor.close()

if __name__ == "__main__":
    investigate()
