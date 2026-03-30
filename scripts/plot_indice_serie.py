import pandas as pd
import plotly.express as px
import pathlib

path = "./output/mean/"


def get_first_df(path):
    """just return a df"""
    data_paths = list(pathlib.Path(path).iterdir())
    df = pd.read_parquet(data_paths[0])
    return df


def get_parcel_id_colums(df: pd.DataFrame):
    """Return the most probable col name for the id of a parcel"""
    valids_col_names = [col for col in df.columns if "id" in col or "parcel" in col]
    col_name = valids_col_names[0]
    return col_name


def plot_data():
    """Plot the data with Plotly"""
    df = get_first_df(path)
    parcel_id_col = get_parcel_id_colums(df)
    fig = px.line(df, "time", "NDVI", color=parcel_id_col)
    fig.show()


if __name__ == "__main__":
    plot_data()
