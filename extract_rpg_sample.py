import geopandas as gpd
from pathlib import Path


def extract_rpg_sample(gpkg_path: str, output_folder: str, count: int = 1):
    """
    Extrait un échantillon de parcelles du RPG officiel.
    Filtres : Code 'BTH', Surface 1-4 ha.
    """
    print(f"--- Extraction de {count} parcelles RPG ('BTH', 1-4 ha) ---")

    # Lecture avec filtre OGR pour aller plus vite (si supporté) ou filtrage pandas
    # surf_parc est en hectares dans le RPG
    try:
        # On lit par morceaux ou avec une requête SQL pour l'efficacité sur un gros fichier
        # Mais ici on va filtrer après lecture d'une partie si possible,
        # ou utiliser l'argument 'where' de pyogrio
        import pyogrio

        # Filtre SQL : code_cultu = 'BTH' AND surf_parc >= 1 AND surf_parc <= 4
        gdf = pyogrio.read_dataframe(
            gpkg_path,
            layer="RPG_Parcelles",
            where="code_cultu = 'BTH' AND surf_parc >= 1 AND surf_parc <= 4",
            max_features=count,
        )

        if gdf.empty:
            print("Aucune parcelle ne correspond aux critères.")
            return

        # On s'assure d'avoir une colonne 'id' pour notre processeur
        if "id_parcel" in gdf.columns:
            gdf = gdf.rename(columns={"id_parcel": "id"})

        # Conversion en 4326 pour être standard
        gdf = gdf.to_crs("EPSG:4326")

        # Sauvegarde
        out_dir = Path(output_folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "rpg_sample_bth.gpkg"

        gdf[["id", "geometry"]].to_file(out_path, driver="GPKG")
        print(f"Succès ! {len(gdf)} parcelles sauvegardées dans {out_path}")

    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")


if __name__ == "__main__":
    rpg_gpkg = "data/extracted/RPG_3-0__GPKG_LAMB93_R76_2024-01-01/RPG/1_DONNEES_LIVRAISON_2024/RPG_3-0__GPKG_LAMB93_R76_2024-01-01/RPG_Parcelles.gpkg"
    # On met ça dans un dossier 2024 pour le test
    extract_rpg_sample(rpg_gpkg, "input/2024-01-01_2024-12-31")
