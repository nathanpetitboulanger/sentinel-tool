import geopandas as gpd
from shapely.geometry import Polygon
from pathlib import Path

def create_sample_gpkg():
    # Create a small polygon (roughly a field in France)
    # Coordinates in 4326
    poly = Polygon([
        (1.44, 43.60),
        (1.45, 43.60),
        (1.45, 43.61),
        (1.44, 43.61),
        (1.44, 43.60)
    ])
    
    gdf = gpd.GeoDataFrame([{'id': 'parcel_1', 'geometry': poly}], crs="EPSG:4326")
    
    # Create input structure
    folder = Path("input/2023-06-01_2023-07-01")
    folder.mkdir(parents=True, exist_ok=True)
    
    gdf.to_file(folder / "parcels.gpkg", driver="GPKG")
    print(f"Created sample GPKG in {folder}")

if __name__ == "__main__":
    create_sample_gpkg()
