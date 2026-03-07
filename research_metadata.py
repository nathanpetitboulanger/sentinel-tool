import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path
from src.config import load_config
from src.processing import SentinelProcessor

def analyze_sentinel_metadata():
    """
    Analyse approfondie des métadonnées Sentinel-2 pour détecter les incohérences 
    de rescale/offset entre anciennes et nouvelles baselines.
    """
    config = load_config()
    processor = SentinelProcessor(config)
    
    # On cherche le premier dossier disponible dans input/
    input_dir = Path("input")
    folder = next(input_dir.iterdir(), None)
    
    if folder is None:
        print("Erreur : Aucun dossier trouvé dans input/")
        return

    print(f"--- Diagnostic Approfondi : {folder.name} ---")
    df = processor.get_diagnostic_metadata(folder)
    
    if df.empty:
        print("Aucune donnée trouvée.")
        return

    # On crée une figure avec 3 graphiques empilés
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 12), sharex=True)
    
    # 1. Offset Radiométrique (B04)
    for sat in df['satellite'].unique():
        subset = df[df['satellite'] == sat]
        ax1.scatter(subset['time'], subset['offset'], label=f"Offset {sat}", alpha=0.6)
    ax1.set_ylabel("Radiometric Offset")
    ax1.set_title("1. Offset Radiométrique (Métadonnée s2:radiometric_offset)")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. Scale Factor
    for sat in df['satellite'].unique():
        subset = df[df['satellite'] == sat]
        ax2.scatter(subset['time'], subset['scale'], label=f"Scale {sat}", marker='x', alpha=0.6)
    ax2.set_ylabel("Scale Factor")
    ax2.set_title("2. Facteur d'échelle (eo:bands scale)")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 3. Flag BOA Offset Applied
    # On transforme le booléen en int (0 ou 1)
    df['boa_flag'] = df['boa_offset_applied'].astype(int)
    for sat in df['satellite'].unique():
        subset = df[df['satellite'] == sat]
        ax3.step(subset['time'], subset['boa_flag'], where='post', label=f"BOA Applied {sat}", alpha=0.8)
    ax3.set_ylabel("Applied (1) / Not (0)")
    ax3.set_yticks([0, 1])
    ax3.set_title("3. Flag 'earthsearch:boa_offset_applied' (Correction automatique par le fournisseur)")
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("output/metadata_diagnostic_full.png")
    print("\nDiagnostic complet sauvegardé dans : output/metadata_diagnostic_full.png")
    
    # Analyse rapide des statistiques
    print("\nRésumé statistique du Scale factor :")
    print(df.groupby('satellite')['scale'].describe())

    processor.close()

if __name__ == "__main__":
    analyze_sentinel_metadata()
