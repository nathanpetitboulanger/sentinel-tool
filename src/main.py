import pandas as pd
from pathlib import Path
from src.config import load_config
from src.processing import SentinelProcessor


def main() -> None:
    """
    Script principal de traitement des données Sentinel-2 en POO.
    """
    # 1. Chargement de la configuration
    config = load_config()

    # 2. Initialisation du processeur (moteur de calcul)
    # L'objet SentinelProcessor gère lui-même son client Dask.
    processor = SentinelProcessor(config)

    input_dir = Path("input")
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    all_dfs = []

    # 3. Parcours des dossiers de dates
    for date_folder in input_dir.iterdir():
        if date_folder.is_dir():
            print(f"--- Traitement du dossier : {date_folder.name} ---")
            results = processor.process_date_folder(date_folder)
            if results:
                all_dfs.extend(results)

    # 4. Fusion des résultats et sauvegarde
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # On ne garde que les dates où au moins un indice est présent
        final_df = final_df.dropna(subset=config["indices"], how="all")

        output_path = output_dir / "sentinel_time_series.parquet"
        final_df.to_parquet(output_path, index=False)
        print(f"Succès ! Résultats sauvegardés dans : {output_path}")
    else:
        print("Aucune donnée n'a pu être traitée.")

    # 5. Nettoyage final
    processor.close()


if __name__ == "__main__":
    main()
