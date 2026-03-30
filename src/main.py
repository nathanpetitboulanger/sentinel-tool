import logging
import pandas as pd
from pathlib import Path
from src.config import load_config, setup_logging
from src.orchestrator import Orchestrator
import dask.distributed
import odc.stac

logger = logging.getLogger(__name__)


def main() -> None:
    """
    Main Sentinel-2 processing script.
    Modular architecture with Orchestrator.
    """
    # 1. Load configuration
    setup_logging()
    config = load_config()

    # 2. Initialize global Dask cluster
    cluster = dask.distributed.LocalCluster(
        n_workers=config["dask"]["n_workers"],
        threads_per_worker=config["dask"]["threads_per_worker"],
    )
    client = dask.distributed.Client(cluster)

    try:
        # Global configuration for speed and unsigned S3 access
        odc.stac.configure_rio(
            cloud_defaults=True, aws={"aws_unsigned": True}, client=client
        )
        logger.info(f"Dask engine ready. Dashboard: {client.dashboard_link}")

        # 3. Initialize Orchestrator
        orchestrator = Orchestrator(config)

        input_dir = Path("input")
        output_dir = Path("output")

        # Create output directory structure
        mean_dir = output_dir / "mean"
        raw_dir = output_dir / "raw"
        mean_dir.mkdir(parents=True, exist_ok=True)

        save_zarr = config.get("save_zarr", False)
        if save_zarr:
            raw_dir.mkdir(parents=True, exist_ok=True)

        all_dfs = []

        # 4. Iterate over date folders
        for date_folder in input_dir.iterdir():
            if date_folder.is_dir() and not date_folder.name.startswith("_"):
                logger.info(f"Processing folder: {date_folder.name}")

                results = orchestrator.process_date_folder(
                    date_folder, raw_dir=raw_dir if save_zarr else None
                )

                if results:
                    all_dfs.extend(results)

        # 5. Merge results and save means
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            indices = config.get("indices", ["NDVI"])
            final_df = final_df.dropna(subset=indices, how="all")

            output_path = mean_dir / "sentinel_time_series.parquet"
            final_df.to_parquet(output_path, index=False)
            logger.info(f"Success! Means saved to: {output_path}")
            if save_zarr:
                logger.info(f"Zarr cubes saved to: {raw_dir}")
        else:
            logger.warning("No data could be processed.")
    finally:
        client.close()
        cluster.close()


if __name__ == "__main__":
    main()
