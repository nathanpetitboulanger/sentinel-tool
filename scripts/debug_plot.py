#!/usr/bin/env python3
"""Generate faceted NDVI plots for parcels rejected by the SCL filter.

Usage:
    uv run python debug_plot.py
    uv run python debug_plot.py --debug-dir debug/
"""

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt
import xarray as xr


def plot_debug_cube(zarr_path: Path) -> None:
    ds = xr.open_zarr(zarr_path)

    if "red" not in ds or "nir" not in ds:
        print(f"[SKIP] {zarr_path.stem} — red/nir bands missing")
        return

    ndvi = (ds.nir - ds.red) / (ds.nir + ds.red)
    scl = ds.scl
    n_times = len(ndvi.time)

    if n_times == 0:
        print(f"[SKIP] {zarr_path.stem} — no dates in the cube")
        return

    n_cols = 6
    n_rows = math.ceil(n_times / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(n_cols * 3, n_rows * 3))
    axes_flat = axes.flatten() if n_times > 1 else [axes]

    for i in range(n_times):
        ax = axes_flat[i]
        date_str = str(ndvi.time[i].values)[:10]
        ndvi.isel(time=i).plot(
            ax=ax, vmin=-1, vmax=1, cmap="RdYlGn", add_colorbar=False
        )
        ax.set_title(date_str, fontsize=8)
        ax.set_xlabel("")
        ax.set_ylabel("")
        ax.tick_params(labelsize=6)

    for j in range(n_times, len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(
        f"Debug Cloud — {zarr_path.stem}\n{n_times} timestamps rejected by SCL filter",
        fontsize=11,
    )
    fig.tight_layout()

    output_png = zarr_path.parent / f"{zarr_path.stem}.png"
    fig.savefig(output_png, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] {zarr_path.stem} — {n_times} dates → {output_png}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Debug NDVI plots for parcels rejected by the SCL filter"
    )
    parser.add_argument(
        "--debug-dir", default="debug/", help="Debug folder (default: debug/)"
    )
    args = parser.parse_args()

    debug_dir = Path(args.debug_dir)
    if not debug_dir.exists():
        print(f"Directory not found: {debug_dir}")
        return

    zarr_paths = sorted(debug_dir.glob("**/*.zarr"))
    if not zarr_paths:
        print(f"No .zarr files found in {debug_dir}")
        return

    print(f"{len(zarr_paths)} debug cube(s) found in {debug_dir}\n")
    for zarr_path in zarr_paths:
        plot_debug_cube(zarr_path)

    print(f"\nDone — {len(zarr_paths)} parcel(s) processed.")


if __name__ == "__main__":
    main()
