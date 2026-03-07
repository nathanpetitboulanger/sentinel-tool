import xarray as xr
from pathlib import Path
from src.config import load_config
from src.processing import SentinelProcessor

def main() -> None:
    config = load_config()
    processor = SentinelProcessor(config)
    
    input_folder = Path("input/2019-01-01_2024-12-31")
    output_zarr = Path("output/sentinel_cube_2019_2024.zarr")
    
    # We'll manually replicate the process_to_zarr logic here to debug
    # but first let's see if we can fix it by cleaning attrs
    
    # Actually let's just use the processor and catch the error to inspect
    try:
        processor.process_to_zarr(input_folder, output_zarr)
    except Exception as e:
        print(f"Error caught: {e}")
        # Let's try to find why it fails
    
    processor.close()

if __name__ == "__main__":
    main()
