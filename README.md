# Sentinel-tool

Pipeline de calcul de séries temporelles d'indices spectraux **Sentinel-2** sur des parcelles agricoles, conçu pour tourner sur un PC portable.

Pour chaque géométries fournie dans `input/` (`.gpkg`), l'outil interroge l'API STAC d'AWS, filtre les pixels nuageux pixel par pixel via la couche SCL, calcule des indices spectraux (NDVI, NDWI, etc.) et produit une série temporelle en sortie (`.parquet`).

Les bornes temporelles de la période d'analyse sont encodées dans le nom du dossier d'entrée, sous la forme `YYYY-MM-DD_YYYY-MM-DD`.

La sortie principale est un fichier Parquet contenant les moyennes spatiales des indices par parcelle et par date. Optionnellement (voir `config.yaml`), les cubes de pixels bruts peuvent être sauvegardés au format `.zarr`(optionnel).

Les journaux d'exécution sont écrits dans `logs/` (horodatés) et affichés en console.

L'outil contient des optimisations comme la réutilisation des items stack ou la parallélisation à tous les niveaux

L'id de parcelle doit être "ID_PARCEL"

Le dashboard Dask est visible pendant l'exécution sur le port 8787

Démo plus bas...

---

## Flux de données

```
input/
  YYYY-MM-DD_YYYY-MM-DD/
    parcelles.gpkg
          │
          ▼
    Découpage spatial (grille de batch_size_deg°)
          │
          ▼
    API STAC → images Sentinel-2 (< 80 % de nuages)
          │
          ▼
    Filtrage SCL au niveux de la parcelle (> 50 % de pixels clairs requis)
          │
          ▼
    Calcul NDVI/NDWI/... → moyenne spatiale par date
          │
          ▼
output/
  mean/sentinel_time_series.parquet
  raw/<parcel_id>.zarr   (optionnel, si save_zarr: true)
```

---

## Installation

Prérequis : **Python 3.12+** et [uv](https://github.com/astral-sh/uv).

```bash
uv sync
```

---

## Démo rapide

Pour lancer un exemple complet avec les données de démonstration incluses :

```bash
uv run python demo.py
```

Ce script copie automatiquement le fichier `.gpkg` de `demo_data/` dans `input/2022-10-01_2023-08-01/`, lance le traitement complet, puis ouvre une visualisation Plotly de la série temporelle NDVI. La configuration `config.yaml` est temporairement modifiée pour la démo et restaurée automatiquement à la fin.

---

## Utilisation

### 1. Préparer les données d'entrée

Placer les fichiers `.gpkg` dans un dossier nommé `YYYY-MM-DD_YYYY-MM-DD` :

```
input/
  2024-04-01_2024-09-30/
    mes_parcelles.gpkg
```

Pour dessiner des parcelles interactivement sur une carte :

```bash
streamlit run scripts/draw_parcel.py
```

### 2. Configurer `config.yaml`

Voir la section [Configuration](#configuration) ci-dessous.

### 3. Lancer le traitement

```bash
uv run python -m src.main
```

Les logs sont écrits dans `logs/` et dans la console.

---

## Configuration

| Clé | Type | Défaut | Description |
|-----|------|--------|-------------|
| `indices` | list | — | Indices à calculer : `NDVI`, `NDWI`, `SAVI`, `NBR`, `NDBI`, `NDRE1`, `NDRE2`, `NDRE3` |
| `stac_api_url` | str | — | URL du catalogue STAC |
| `collection` | str | — | Collection Sentinel-2 (ex: `sentinel-2-c1-l2a`) (Ne pas utiliser sentinel-2-l2a à part si vous savez ce que vous faites) |
| `dask.n_workers` | int | — | Nombre de workers Dask |
| `dask.threads_per_worker` | int | — | Threads par worker |
| `save_zarr` | bool | `false` | Sauvegarder les cubes bruts en `.zarr` |
| `debug_cloud` | bool | `false` | Sauvegarder les cubes de debug pour les parcelles rejetées |
| `batch_size_deg` | float | `0.5` | Taille du découpage spatial en degrés |

Exemple minimal :

```yaml
indices: [NDVI, NDWI]
stac_api_url: "https://earth-search.aws.element84.com/v1"
collection: "sentinel-2-c1-l2a"
dask:
  n_workers: 4
  threads_per_worker: 10
```

---

## Sorties

| Fichier | Description |
|---------|-------------|
| `output/mean/sentinel_time_series.parquet` | Série temporelle moyennée par parcelle |
| `output/raw/<parcel_id>.zarr` | Cube brut par parcelle (optionnel) |

Colonnes du Parquet : `time`, `parcel_id`, `source_file`, `date_range`, + valeurs des indices.

---

## Architecture

| Classe | Fichier | Rôle |
|--------|---------|------|
| `StacFetcher` | `src/stac_fetcher.py` | Requêtes STAC avec retries et découpage des périodes > 1 an |
| `MathEngine` | `src/math_engine.py` | Filtrage SCL, calcul des indices, moyennes spatiales |
| `Orchestrator` | `src/orchestrator.py` | Découpage spatial, parallélisation avec `ThreadPoolExecutor` |
| `main` | `src/main.py` | Point d'entrée, initialisation Dask |

---

## Scripts utilitaires

| Script | Description |
|--------|-------------|
| `scripts/draw_parcel.py` | Interface Streamlit pour dessiner des parcelles sur une carte |
| `scripts/plot_indice_serie.py` | Visualisation Plotly de la série temporelle |
| `scripts/debug_plot.py` | Visualisation des masques SCL pour les parcelles rejetées |

---

## Tests

```bash
uv run pytest
```
