import concurrent.futures
import warnings
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape

from src.stac_fetcher import StacFetcher
from src.math_engine import MathEngine

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Logistics expert.
    Reads geospatial files, creates spatial batches, and coordinates
    StacFetcher (network) and MathEngine (computation).
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.fetcher = StacFetcher(config["stac_api_url"], config["collection"])
        self.engine = MathEngine(config["indices"])

    def _process_batch(
        self,
        batch_idx: int,
        batch_id: str,
        batch_gdf: gpd.GeoDataFrame | pd.DataFrame,
        start_date: str,
        end_date: str,
        folder_path: Path,
        gpkg_file: Path,
        raw_dir: Path | None,
    ) -> List[pd.DataFrame]:
        """Process an entire spatial batch (STAC query + parcel processing)."""
        logger.info(
            f"[Batch {batch_idx + 1}] START Zone {batch_id}: {len(batch_gdf)} parcels"
        )

        batch_bbox: List[float] = batch_gdf["geom_wgs84"].total_bounds.tolist()

        # The Fetcher handles network and temporal complexity
        all_stac_items = self.fetcher.fetch_items_for_batch(
            bbox=batch_bbox, start_date=start_date, end_date=end_date
        )

        if not all_stac_items:
            logger.warning(
                f"[Batch {batch_idx + 1}] No STAC items found for zone {batch_id}."
            )
            return []

        logger.info(
            f"[Batch {batch_idx + 1}] {len(all_stac_items)} STAC items retrieved. Starting computation..."
        )

        # Pre-compute STAC geometries
        items_with_geom = [
            (item, shape(item.geometry))
            for item in all_stac_items
            if item.geometry is not None
        ]
        debug_cloud: bool = self.config.get("debug_cloud", False)
        batch_results = []

        # --- OPTIMISATION: LOCAL UTM CRS ---
        # Compute the optimal UTM CRS only for this small ~50km batch
        batch_wgs84 = gpd.GeoDataFrame(
            geometry=list(batch_gdf["geom_wgs84"]), crs="EPSG:4326"
        )
        utm_crs = batch_wgs84.estimate_utm_crs()
        batch_gdf_utm = batch_gdf.to_crs(utm_crs)

        # --- PARALLEL PARCEL PROCESSING (within the batch) ---
        def process_single_row(idx_and_row: Tuple[Any, pd.Series]):
            idx, row = idx_and_row

            parcel_id = row["ID_PARCEL"]

            row = row.copy()
            row["id"] = parcel_id
            meta = {"source_file": gpkg_file.name, "date_range": folder_path.name}

            if raw_dir:
                save_path = raw_dir / folder_path.name / f"{parcel_id}.zarr"
            else:
                save_path = None

            debug_path: Optional[Path] = None
            if debug_cloud:
                debug_path = Path("debug_data") / folder_path.name / f"{parcel_id}.zarr"

            # Filtrage spatial
            parcel_geom_wgs84 = row["geom_wgs84"]

            intersecting_items = [
                item
                for item, item_geom in items_with_geom
                if parcel_geom_wgs84.intersects(item_geom)
            ]

            if not intersecting_items:
                logger.warning(
                    f"[{parcel_id}] Parcel extent does not intersect any STAC image."
                )
                return None

            # Temporal filtering (SCL)
            local_bbox = list(row.geometry.bounds)
            usable_items = self.engine.get_usable_timestamps(
                intersecting_items, local_bbox, row.geometry, utm_crs
            )

            if not usable_items:
                if debug_path is not None:
                    self.engine.save_debug_cube(
                        intersecting_items, row, utm_crs, debug_path
                    )
                return None

            # Final computation (uses local UTM geometry)
            return self.engine.process_parcel_data(
                usable_items, row, utm_crs, meta, save_cube_path=save_path
            )

        # Iterate over the UTM DataFrame
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_single_row, item)
                for item in batch_gdf_utm.iterrows()
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    if res is not None:
                        batch_results.append(res)
                except Exception as e:
                    logger.exception(
                        f"[Batch {batch_idx + 1}] Unexpected error on a parcel."
                    )

        logger.info(
            f"[Batch {batch_idx + 1}] DONE ({len(batch_results)} parcels processed)"
        )
        return batch_results

    def process_date_folder(
        self, folder_path: Path, raw_dir: Optional[Path] = None
    ) -> List[pd.DataFrame]:
        dates = folder_path.name.split("_")
        if len(dates) != 2:
            return []

        start_date, end_date = dates
        all_results = []

        for gpkg_file in folder_path.glob("*.gpkg"):
            gdf = gpd.read_file(gpkg_file)
            if gdf.empty:
                continue
            if "ID_PARCEL" not in gdf.columns:
                raise ValueError(
                    f"{gpkg_file.name}: missing required column 'ID_PARCEL'. "
                    f"Got: {gdf.columns.tolist()}"
                )
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)

            logger.info(f"Analysing file {gpkg_file.name} ({len(gdf)} parcels)")

            # --- 1. SPATIAL BATCHING (grid creation) ---
            gdf_wgs84 = gdf.to_crs("EPSG:4326")

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                centroids = gdf_wgs84.geometry.centroid

            batch_size = self.config["batch_size_deg"]
            gdf["batch_id"] = (centroids.y / batch_size).round() * batch_size
            gdf["batch_id"] = (
                gdf["batch_id"].astype(str)
                + "_"
                + ((centroids.x / batch_size).round() * batch_size).astype(str)
            )

            # Store the WGS84 geometry in the original DataFrame
            gdf["geom_wgs84"] = gdf_wgs84.geometry

            batches = list(gdf.groupby("batch_id"))

            logger.info(
                f"Split into {len(batches)} spatial batches. Parallelisation enabled."
            )

            # --- 2. PARALLEL BATCH PROCESSING ---
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as batch_executor:
                batch_futures = []
                for batch_idx, (batch_id, batch_gdf) in enumerate(batches):
                    future = batch_executor.submit(
                        self._process_batch,
                        batch_idx,
                        str(batch_id),
                        batch_gdf,
                        start_date,
                        end_date,
                        folder_path,
                        gpkg_file,
                        raw_dir,
                    )
                    batch_futures.append(future)

                for future in concurrent.futures.as_completed(batch_futures):
                    try:
                        batch_res = future.result()
                        all_results.extend(batch_res)
                    except Exception as e:
                        logger.exception(f"Fatal error on a complete batch.")

        return all_results
