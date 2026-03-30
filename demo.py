import shutil
import subprocess
import yaml
from pathlib import Path


CONFIG_PATH = Path("config.yaml")
INPUT_DIR = Path("input/2022-10-01_2023-08-01")
DEMO_GPKG = Path("demo_data/rpg_extrait_bth_test.gpkg")


def prepare_input() -> None:
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DEMO_GPKG, INPUT_DIR / DEMO_GPKG.name)
    print(f"Demo data copied to {INPUT_DIR / DEMO_GPKG.name}")


def main() -> None:
    # 1. Prepare input
    prepare_input()

    # 2. Patch config.yaml for the demo
    original_config = CONFIG_PATH.read_text()
    config = yaml.safe_load(original_config)
    config["indices"] = ["NDVI"]
    config["save_zarr"] = False
    CONFIG_PATH.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True)
    )

    try:
        # 3. Run main pipeline
        subprocess.run(["uv", "run", "python", "-m", "src.main"], check=True)

        # 4. Plot results
        subprocess.run(
            ["uv", "run", "python", "scripts/plot_indice_serie.py"], check=True
        )
    finally:
        # 5. Restore original config
        CONFIG_PATH.write_text(original_config)
        print("config.yaml restored.")


if __name__ == "__main__":
    main()
