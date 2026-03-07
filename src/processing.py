from pathlib import Path
from typing import Any, Dict, List, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
import pystac_client
import rioxarray
import stackstac
import xarray as xr
from dask.distributed import Client
from pyproj import CRS


class SentinelProcessor:
    """
    Processeur orienté objet pour le traitement des séries temporelles Sentinel-2.
    Gère la connexion STAC, le filtrage SCL et le calcul d'indices via Dask.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialise le processeur avec la configuration et le client Dask.
        """
        self.config = config
        self.indices = config["indices"]
        self.stac_url = config["stac_api_url"]
        self.collection = config["collection"]
        self.catalog = pystac_client.Client.open(self.stac_url)

        # Initialisation du client Dask comme moteur interne
        self.client = Client(
            n_workers=config["dask"]["n_workers"],
            threads_per_worker=config["dask"]["threads_per_worker"],
        )
        print(
            f"Moteur SentinelProcessor prêt. Dashboard Dask : {self.client.dashboard_link}"
        )

    def _calculate_indices(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Calcule les indices NDVI/NDWI de manière robuste.
        """
        res = {}
        bands = ds.band.values.tolist()

        if "NDVI" in self.indices and "nir" in bands and "red" in bands:
            nir = ds.sel(band="nir")
            red = ds.sel(band="red")
            denom = nir + red
            res["NDVI"] = xr.where(denom != 0, (nir - red) / denom, np.nan)

        if "NDWI" in self.indices and "green" in bands and "nir" in bands:
            green = ds.sel(band="green")
            nir = ds.sel(band="nir")
            denom = green + nir
            res["NDWI"] = xr.where(denom != 0, (green - nir) / denom, np.nan)

        return xr.Dataset(res)

    def _get_usable_timestamps(
        self,
        items: pystac.ItemCollection,
        bbox: List[float],
        geometry: Any,
        crs: CRS,
        threshold: float = 0.5,
    ) -> pd.DatetimeIndex:
        """
        Analyse la couche SCL pour identifier les dates exploitables sur la parcelle.
        Utilise Dask pour le calcul de la moyenne spatiale SCL.
        """
        scl_stack = stackstac.stack(
            items,
            bounds=bbox,
            assets=["scl"],
            epsg=crs.to_epsg(),
            resolution=20,
            dtype="uint8",
            fill_value=0,
            chunksize=1024,
        )

        # Filtrage SCL (4: Veg, 5: Soil, 6: Water, 7: Unclassified)
        valid_scl = [4, 5, 6, 7]
        clipped_scl = scl_stack.rio.clip([geometry], crs)

        is_valid_pixel = clipped_scl.sel(band="scl").isin(valid_scl)
        # Déclenchement du calcul Dask pour le ratio de pixels valides
        valid_ratio = is_valid_pixel.mean(dim=["x", "y"]).compute()

        keep_times = valid_ratio.time[valid_ratio > threshold].values
        return pd.to_datetime(keep_times)

    def process_parcel(
        self,
        items: pystac.ItemCollection,
        row: pd.Series,
        bbox_utm: List[float],
        crs: CRS,
        meta: Dict[str, str],
    ) -> Optional[pd.DataFrame]:
        """
        Traitement complet d'une parcelle : SCL -> Filtrage -> Indices.
        """
        keep_times = self._get_usable_timestamps(items, bbox_utm, row.geometry, crs)

        if len(keep_times) == 0:
            print(f"Pas de données valides (SCL) pour la parcelle {row.get('id')}")
            return None

        # Filtrage des items STAC pour ne charger que le nécessaire
        selected_items = [
            item
            for item in items
            if any(
                np.abs(
                    (
                        item.datetime.replace(tzinfo=None)
                        - t.to_pydatetime().replace(tzinfo=None)
                    ).total_seconds()
                )
                < 1
                for t in keep_times
            )
        ]

        if not selected_items:
            return None

        # Chargement des bandes spectrales (10m) via Dask
        stack = stackstac.stack(
            selected_items,
            bounds=bbox_utm,
            assets=["green", "red", "nir"],
            epsg=crs.to_epsg(),
            resolution=10,
            dtype="float32",
            fill_value=np.float32(np.nan),
            chunksize=1024,
        ).rio.write_crs(crs)

        # Extraction de la moyenne spatiale et calcul des indices
        mean_bands = stack.rio.clip([row.geometry], crs).mean(dim=["x", "y"]).compute()
        indices_ds = self._calculate_indices(mean_bands)

        df = indices_ds.to_dataframe().reset_index()

        # Injection des métadonnées
        df["parcel_id"] = row.get("id")
        df["source_file"] = meta["source_file"]
        df["date_range"] = meta["date_range"]

        cols = ["time"] + self.indices + ["parcel_id", "source_file", "date_range"]
        return df[[c for c in cols if c in df.columns]]

    def process_date_folder(self, folder_path: Path) -> List[pd.DataFrame]:
        """
        Orchestre le traitement d'un dossier de dates.
        """
        dates = folder_path.name.split("_")
        if len(dates) != 2:
            return []

        start_date, end_date = dates
        all_results = []
        catalog = pystac_client.Client.open(self.stac_url)

        for gpkg_file in folder_path.glob("*.gpkg"):
            gdf = gpd.read_file(gpkg_file)
            if gdf.empty:
                continue

            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)

            utm_crs = gdf.estimate_utm_crs()
            gdf_utm = gdf.to_crs(utm_crs)

            search = catalog.search(
                collections=[self.collection],
                bbox=gdf.to_crs("EPSG:4326").total_bounds.tolist(),
                datetime=f"{start_date}/{end_date}",
                query={"eo:cloud_cover": {"lt": 20}},
            )

            items = search.item_collection()
            print(f"Trouvé {len(items)} items bruts pour {gpkg_file.name}")

            if not items:
                continue

            # --- DÉDUPLICATION ET HARMONISATION ---
            dedup_dict = {}
            for item in items:
                date_key = item.datetime.strftime("%Y-%m-%dT%H:%M:%S")
                tile_key = item.properties.get("s2:mgrs_tile", "unknown")
                key = f"{date_key}_{tile_key}"
                is_boa = item.properties.get("earthsearch:boa_offset_applied", False)
                if key not in dedup_dict:
                    dedup_dict[key] = item
                else:
                    old_is_boa = dedup_dict[key].properties.get(
                        "earthsearch:boa_offset_applied", False
                    )
                    if is_boa and not old_is_boa:
                        dedup_dict[key] = item

            filtered_items = pystac.ItemCollection(list(dedup_dict.values()))
            print(
                f"Après déduplication : {len(filtered_items)} items uniques conservés."
            )

            for idx, row in gdf_utm.iterrows():
                if row.get("id") is None:
                    row["id"] = f"{gpkg_file.stem}_{idx}"

                meta = {"source_file": gpkg_file.name, "date_range": folder_path.name}
                res = self.process_parcel(
                    filtered_items, row, gdf_utm.total_bounds.tolist(), utm_crs, meta
                )

                if res is not None:
                    all_results.append(res)

        return all_results

    def process_to_zarr(self, folder_path: Path, output_path: Path) -> None:
        """
        Traite un dossier et exporte le cube d'indices complet (x,y,time) en Zarr.
        """
        dates = folder_path.name.split("_")
        if len(dates) != 2:
            return

        start_date, end_date = dates
        catalog = pystac_client.Client.open(self.stac_url)

        gpkgs = list(folder_path.glob("*.gpkg"))
        if not gpkgs:
            return

        gdf_total = pd.concat([gpd.read_file(g) for g in gpkgs])
        if gdf_total.crs is None:
            gdf_total.set_crs("EPSG:4326", inplace=True)
        utm_crs = gdf_total.estimate_utm_crs()
        gdf_utm = gdf_total.to_crs(utm_crs)

        search = catalog.search(
            collections=[self.collection],
            bbox=gdf_total.to_crs("EPSG:4326").total_bounds.tolist(),
            datetime=f"{start_date}/{end_date}",
            query={"eo:cloud_cover": {"lt": 20}},
        )
        items = search.item_collection()
        if not items:
            return

        # Déduplication
        dedup_dict = {}
        for item in items:
            date_key = item.datetime.strftime("%Y-%m-%dT%H:%M:%S")
            tile_key = item.properties.get("s2:mgrs_tile", "unknown")
            key = f"{date_key}_{tile_key}"
            is_boa = item.properties.get("earthsearch:boa_offset_applied", False)
            if key not in dedup_dict:
                dedup_dict[key] = item
            else:
                old_is_boa = dedup_dict[key].properties.get(
                    "earthsearch:boa_offset_applied", False
                )
                if is_boa and not old_is_boa:
                    dedup_dict[key] = item

        unique_items = list(dedup_dict.values())

        stack = stackstac.stack(
            unique_items,
            bounds=gdf_utm.total_bounds.tolist(),
            assets=["green", "red", "nir"],
            epsg=utm_crs.to_epsg(),
            resolution=10,
            dtype="float32",
            fill_value=np.float32(np.nan),
            chunksize=1024,
        ).rio.write_crs(utm_crs)

        indices_ds = self._calculate_indices(stack)

        print(
            f"Exportation du cube Zarr ({len(unique_items)} dates) vers : {output_path}..."
        )
        indices_ds.to_zarr(output_path, mode="w")
        print("Exportation Zarr terminée.")

    def close(self):
        """Ferme proprement le client Dask."""
        self.client.close()
