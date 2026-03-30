import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
import pystac
import rioxarray
import odc.stac
import xarray as xr
from pyproj import CRS

logger = logging.getLogger(__name__)


class MathEngine:
    """
    Computation expert responsible for pixel processing.
    Handles S3 loading, clipping, SCL masking, and index calculation.
    """

    def __init__(self, indices: List[str]):
        self.indices = indices

    def _scale_dataset(self, ds: xr.Dataset, items: List[pystac.Item]) -> xr.Dataset:
        raster_info = items[0].assets["red"].extra_fields.get("raster:bands", [{}])[0]
        scale = raster_info.get("scale", 0.0001)
        offset = raster_info.get("offset", 0)
        ds_scaled = (ds.drop_vars("scl").astype("float32") * scale) + offset
        ds_scaled["scl"] = ds["scl"]
        return ds_scaled

    def _calculate_indices(self, ds: xr.Dataset) -> xr.Dataset:
        res = {}
        if "NDVI" in self.indices and "nir" in ds and "red" in ds:
            res["NDVI"] = (ds.nir - ds.red) / (ds.nir + ds.red)
        if "NDWI" in self.indices and "green" in ds and "nir" in ds:
            res["NDWI"] = (ds.green - ds.nir) / (ds.green + ds.nir)
        if "SAVI" in self.indices and "nir" in ds and "red" in ds:
            L = 0.5
            res["SAVI"] = (ds.nir - ds.red) / (ds.nir + ds.red + L) * (1 + L)
        if "NBR" in self.indices and "nir" in ds and "swir16" in ds:
            res["NBR"] = (ds.nir - ds.swir16) / (ds.nir + ds.swir16)
        if "NDBI" in self.indices and "swir16" in ds and "nir" in ds:
            res["NDBI"] = (ds.swir16 - ds.nir) / (ds.swir16 + ds.nir)
        if "NDRE1" in self.indices and "nir" in ds and "rededge1" in ds:
            res["NDRE1"] = (ds.nir - ds.rededge1) / (ds.nir + ds.rededge1)
        if "NDRE2" in self.indices and "nir" in ds and "rededge2" in ds:
            res["NDRE2"] = (ds.nir - ds.rededge2) / (ds.nir + ds.rededge2)
        if "NDRE3" in self.indices and "nir" in ds and "rededge3" in ds:
            res["NDRE3"] = (ds.nir - ds.rededge3) / (ds.nir + ds.rededge3)
        return xr.Dataset(res)

    def get_usable_timestamps(
        self,
        items: List[pystac.Item],
        bbox: List[float],
        geometry: Any,
        crs: CRS,
        threshold: float = 0.5,
    ) -> List[pystac.Item]:
        """
        Filter out timesteps that are too cloudy.
        """

        if not items:
            return []
        min_x, min_y, max_x, max_y = bbox

        ds_scl = odc.stac.load(
            items,
            bands=["scl"],
            crs=f"EPSG:{crs.to_epsg()}",
            resolution=20,
            x=(min_x - 20, max_x + 20),
            y=(min_y - 20, max_y + 20),
            groupby="solar_day",
            chunks={},
            fail_on_error=True,
        )

        if ds_scl is None or "scl" not in ds_scl:
            logger.warning("SCL: Load failed (missing data or S3 error)")
            return []

        clipped_scl = ds_scl.rio.write_crs(crs).rio.clip([geometry], crs.to_string())
        scl = clipped_scl.scl
        mask_data = scl != 0  # pixels inside the extent (nodata = 0 after clip)
        mask_valid = scl.isin([4, 5])
        valid_ratio = mask_valid.where(mask_data).mean(dim=["x", "y"]).compute()

        usable_mask = valid_ratio > threshold
        count_ok = int(usable_mask.sum())
        total_dates = len(valid_ratio.time)

        if count_ok == 0:
            max_r = float(valid_ratio.max()) if total_dates > 0 else 0
            logger.warning(
                f"SCL: 0/{total_dates} valid dates (Max: {max_r:.2f}, Required: {threshold})"
            )
            return []

        logger.info(
            f"SCL: {count_ok}/{total_dates} dates kept (Mean ratio: {float(valid_ratio[usable_mask].mean()):.2f})"
        )

        usable_times = valid_ratio.time[usable_mask].values
        usable_times_dt = pd.to_datetime(usable_times)
        usable_days = {t.strftime("%Y-%m-%d") for t in usable_times_dt}

        return [
            item
            for item in items
            if (d := item.datetime) and d.strftime("%Y-%m-%d") in usable_days
        ]

    def save_debug_cube(
        self,
        items: List[pystac.Item],
        row: pd.Series,
        crs: CRS,
        debug_path: Path,
    ) -> None:
        """Save a raw (unmasked) datacube for a parcel rejected by the SCL filter."""
        parcel_id = row["id"]
        local_bbox = list(row.geometry.bounds)
        min_x, min_y, max_x, max_y = local_bbox

        ds = odc.stac.load(
            items,
            bands=["red", "nir", "scl"],
            crs=f"EPSG:{crs.to_epsg()}",
            resolution=10,
            x=(min_x - 10, max_x + 10),
            y=(min_y - 10, max_y + 10),
            groupby="solar_day",
            chunks={},
            fail_on_error=True,
        )

        if ds is None or "red" not in ds:
            logger.warning(f"[{parcel_id}] Debug: failed to load spectral bands.")
            return

        ds_scaled = self._scale_dataset(ds, items)

        ds_clipped = ds_scaled.rio.write_crs(crs).rio.clip(
            [row.geometry], crs.to_string()
        )

        debug_path.parent.mkdir(parents=True, exist_ok=True)
        ds_clipped.to_zarr(debug_path, mode="w")
        logger.info(
            f"[{parcel_id}] Debug cube saved ({len(ds_clipped.time)} dates) → {debug_path}"
        )

    def process_parcel_data(
        self,
        selected_items: List[pystac.Item],
        row: pd.Series,
        crs: CRS,
        meta: Dict[str, str],
        save_cube_path: Optional[Path] = None,
    ) -> Optional[pd.DataFrame]:
        parcel_id = row["id"]
        local_bbox = list(row.geometry.bounds)
        min_x, min_y, max_x, max_y = local_bbox

        logger.info(
            f"[{parcel_id}] Loading HR bands (10m) for {len(selected_items)} dates..."
        )
        bands = ["red", "green", "blue", "nir", "scl"]
        if any(idx in self.indices for idx in ("NBR", "NDBI")):
            bands.append("swir16")
        if "NDRE1" in self.indices:
            bands.append("rededge1")
        if "NDRE2" in self.indices:
            bands.append("rededge2")
        if "NDRE3" in self.indices:
            bands.append("rededge3")

        ds = odc.stac.load(
            selected_items,
            bands=bands,
            crs=f"EPSG:{crs.to_epsg()}",
            resolution=10,
            x=(min_x - 10, max_x + 10),
            y=(min_y - 10, max_y + 10),
            groupby="solar_day",
            chunks={},
            fail_on_error=True,
        )

        if ds is None or "red" not in ds:
            logger.error(f"[{parcel_id}] Failed to load spectral data.")
            return None

        ds_scaled = self._scale_dataset(ds, selected_items)

        ds_clipped = ds_scaled.rio.write_crs(crs).rio.clip(
            [row.geometry], crs.to_string()
        )

        mask_bool = ds_clipped.scl.isin([4, 5])
        ds_final = ds_clipped.where(mask_bool)

        indices_ds = self._calculate_indices(ds_final)
        ds_full = xr.merge([ds_final.drop_vars("scl"), indices_ds])

        if save_cube_path:
            save_cube_path.parent.mkdir(parents=True, exist_ok=True)
            ds_full.attrs.update(meta)
            ds_full.to_zarr(save_cube_path, mode="w")
            logger.info(f"[{parcel_id}] Zarr cube saved ({len(ds_full.time)} dates)")

        mean_bands = ds_full.mean(dim=["x", "y"]).compute()
        df = mean_bands.to_dataframe().reset_index()

        df["parcel_id"] = parcel_id
        df["source_file"] = meta["source_file"]
        df["date_range"] = meta["date_range"]

        logger.info(f"[{parcel_id}] Time series generated ({len(df)} points)")
        return df
