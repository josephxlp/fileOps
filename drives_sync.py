from udrive_sync import sync_directories
import logging
logging.basicConfig(level=logging.INFO)

# Example usage
source = "/path/to/source"
destination = "/path/to/destination"


source = "/media/ljp238/12TBWolf/SPPHDU3/"
destination = "/media/ljp238/12TDX/ARXIV/SPPHDU3/"

try:
    exit_code = sync_directories(
        src=source,
        dst=destination,
        mode="fast",               # or "safe", "aggressive"
        parallel_workers=10,
        verbose=True
    )
    if exit_code == 0:
        print("Sync succeeded!")
    else:
        print(f"Sync failed with exit code {exit_code}")
except Exception as e:
    print(f"Fatal error: {e}")







