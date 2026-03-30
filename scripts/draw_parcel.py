import streamlit as st
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
import geopandas as gpd
from shapely.geometry import shape
import os
from datetime import date

# Page configuration
st.set_page_config(page_title="Sat-Sentinel Parcel Drawer", layout="wide")
st.title("🛰️ Sat-Sentinel: Parcel Drawer")

# Configuration form
with st.sidebar:
    st.header("1. Parameters")
    start_date = st.date_input("Start date", date(2024, 5, 1))
    end_date = st.date_input("End date", date(2024, 6, 1))

    parcel_name = st.text_input("Parcel name", "my_parcel")

    # Explicit basemap selection
    map_type = st.radio(
        "Map type",
        ["Satellite (Google)", "Satellite (ESRI)", "Map (OSM)"],
        index=0,
    )

    folder_name = f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
    st.info(f"Target folder: `input/{folder_name}/`")

st.header("2. Draw the parcel on the map")
st.write("Use the tools on the left to draw a polygon or rectangle.")

# Basemap dictionary
tiles_dict = {
    "Satellite (Google)": {
        "url": "https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}",
        "attr": "Google",
    },
    "Satellite (ESRI)": {
        "url": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
        "attr": "Esri",
    },
    "Map (OSM)": {"url": "openstreetmap", "attr": "OpenStreetMap"},
}

# Initialize map with user's basemap choice
m = folium.Map(
    location=[46.5, 2.5],
    zoom_start=6,
    tiles=tiles_dict[map_type]["url"],
    attr=tiles_dict[map_type]["attr"],
)

# Add drawing tool
Draw(
    export=False,
    draw_options={
        "polyline": False,
        "rectangle": True,
        "polygon": True,
        "circle": False,
        "marker": False,
        "circlemarker": False,
    },
).add_to(m)

# Display interactive map
# Note: dynamic key forces a reload when the map type changes
output = st_folium(m, width=1200, height=600, key=f"map_{map_type}")

# Process result
if output and output.get("all_drawings"):
    drawings = output["all_drawings"]
    count = len(drawings)

    st.success(f"✅ {count} geometry(ies) detected.")

    if st.button("Save parcel as GPKG"):
        # Convert drawings to GeoDataFrame
        geoms = [shape(d["geometry"]) for d in drawings]
        gdf = gpd.GeoDataFrame(
            {"id": range(len(geoms)), "name": [parcel_name] * len(geoms)},
            geometry=geoms,
            crs="EPSG:4326",
        )

        # Prepare output directory
        output_dir = os.path.join("input", folder_name)
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"{parcel_name}.gpkg")

        # Save
        gdf.to_file(file_path, driver="GPKG")
        st.balloons()
        st.success(f"Parcel saved successfully to: `{file_path}`")
        st.code(f"ls {file_path}")
