import pandas as pd
import plotly.express as px
import plotly.io as pio
import xarray as xr
import matplotlib.pyplot as plt
import geopandas as gpd
import os

pio.renderers.default = "browser"

df = pd.read_parquet("./output/sentinel_time_series.parquet")

df.columns

px.scatter(df, "time", "NDVI")


test = gpd.read_file("./input/2024-01-01_2024-12-31/rpg_sample_bth.gpkg")
