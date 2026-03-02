import pandas as pd
import plotly.express as px
import plotly.io as pio
import xarray as xr
import matplotlib.pyplot as plt

pio.renderers.default = "browser"


ds = (
    xr.open_dataset("ds.zarr")
    .drop_duplicates("time")
    .sortby("time")
    .to_dataarray()
    .squeeze()
    .to_dataset(dim="band")
    .clip(min=0.01)
)

ds["ndvi"] = (ds["nir"] - ds["red"]) / (ds["nir"] + ds["red"])


ds["ndvi"].isel(time=slice(20, 60)).plot(
    col="time",
    col_wrap=10,
    vmax=1,
    vmin=0,
)
plt.show()

ds


df = pd.read_parquet("output/sentinel_time_series.parquet")

px.scatter(df, "time", "NDVI")


from matplotlib.font_manager import findfont, FontProperties

# L'astuce : mettre ['sans-serif'] entre crochets pour éviter le bug du parser
prop = FontProperties(family=["sans-serif"])
path = findfont(prop)

print(f"Le fichier police utilisé est : {path}")
