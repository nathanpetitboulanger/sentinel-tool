import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
from src.config import load_config
from src.processing import SentinelProcessor
import geopandas as gpd
import xarray as xr
import stackstac
import pystac

def compare_ndvi_logic():
    config = load_config()
    processor = SentinelProcessor(config)
    
    # On prend une parcelle de 2024
    folder = Path("input/2024-01-01_2024-12-31")
    gpkg = next(folder.glob("*.gpkg"))
    gdf = gpd.read_file(gpkg)
    utm_crs = gdf.estimate_utm_crs()
    gdf_utm = gdf.to_crs(utm_crs)
    row = gdf_utm.iloc[0]
    
    search = processor.catalog.search(
        collections=[config["collection"]],
        bbox=gdf.to_crs("EPSG:4326").total_bounds.tolist(),
        datetime="2024-01-01/2024-12-31",
        query={"eo:cloud_cover": {"lt": 10}},
    )
    items = search.item_collection()
    
    # Subset pour rapidité
    items = items[:15]

    # Déduplication
    dedup_dict = {}
    for item in items:
        key = f"{item.datetime.strftime('%Y-%m-%dT%H:%M:%S')}_{item.properties.get('s2:mgrs_tile')}"
        if key not in dedup_dict or item.properties.get('earthsearch:boa_offset_applied', False):
            dedup_dict[key] = item
    
    unique_items = list(dedup_dict.values())
    
    stack = stackstac.stack(
        unique_items,
        bounds=gdf_utm.total_bounds.tolist(),
        assets=["red", "nir"],
        epsg=utm_crs.to_epsg(),
        resolution=10,
        dtype="float32",
        fill_value=np.float32(np.nan),
        chunksize=1024,
    ).rio.write_crs(utm_crs)

    mean_bands = stack.rio.clip([row.geometry], utm_crs).mean(dim=["x", "y"]).compute()
    
    red = mean_bands.sel(band="red")
    nir = mean_bands.sel(band="nir")
    
    # NDVI 1 : Brut (As-is)
    # On ajoute un epsilon pour éviter div par zero
    ndvi_as_is = (nir - red) / (nir + red + 1e-6)
    
    # NDVI 2 : Unshifted (on rajoute 0.1 car boa_offset_applied est True sur ces items)
    # L'offset de 1000 équivaut à 0.1 de réflectance
    # ( (NIR+0.1) - (Red+0.1) ) / ( (NIR+0.1) + (Red+0.1) ) = (NIR - Red) / (NIR + Red + 0.2)
    ndvi_unshifted = (nir - red) / (nir + red + 0.2 + 1e-6)
    
    plt.figure(figsize=(12, 6))
    plt.plot(mean_bands.time, ndvi_as_is, 'r-o', label="NDVI 'As-Is' (Fournisseur, offset -0.1)")
    plt.plot(mean_bands.time, ndvi_unshifted, 'g-s', label="NDVI 'Unshifted' (Corrigé, +0.1)")
    
    plt.axhline(1.0, color='black', linestyle='--', alpha=0.5)
    plt.axhline(0.0, color='black', linestyle='-', alpha=0.2)
    plt.ylim(-0.5, 1.5)
    plt.title(f"Impact de l'offset BOA sur le NDVI - 2024 - Parcelle {row.id}")
    plt.ylabel("Valeur NDVI")
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.savefig("output/compare_ndvi_rescale.png")
    print("Graphique de comparaison généré : output/compare_ndvi_rescale.png")
    
    # Print quelques valeurs pour voir
    comparison = pd.DataFrame({
        'time': mean_bands.time.values,
        'red_raw': red.values,
        'nir_raw': nir.values,
        'ndvi_as_is': ndvi_as_is.values,
        'ndvi_unshifted': ndvi_unshifted.values
    })
    print("\nÉchantillon de valeurs :")
    print(comparison.head(10))

    processor.close()

if __name__ == "__main__":
    compare_ndvi_logic()
