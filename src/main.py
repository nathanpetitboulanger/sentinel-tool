import pandas as pd
from pathlib import Path
from dask.distributed import Client
from config import load_config
from processing import process_date_folder


def main() -> None:
    """Main entry point for the Sentinel processing pipeline."""
    config = load_config()

    # Initialize Dask client
    client = Client(
        n_workers=config["dask"]["n_workers"],
        threads_per_worker=config["dask"]["threads_per_worker"],
    )
    print(f"Dask dashboard available at: {client.dashboard_link}")

    input_dir = Path("input")
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    all_dfs = []

    # Iterate over date-named folders
    for date_folder in input_dir.iterdir():
        if date_folder.is_dir():
            print(f"Processing folder: {date_folder.name}")
            results = process_date_folder(date_folder, config)
            all_dfs.extend(results)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # Drop rows with NaN if no data was found for some dates
        final_df = final_df.dropna(subset=config["indices"], how="all")

        output_path = output_dir / "sentinel_time_series.parquet"
        final_df.to_parquet(output_path, index=False)
        print(f"Successfully saved results to {output_path}")
    else:
        print("No data processed.")


if __name__ == "__main__":
    main()
