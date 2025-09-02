import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

def run_rclone(src, dst):
    """Run rclone copy command with progress."""
    subprocess.run(["rclone", "copy", src, dst, "--progress"], check=True)

def upload_path_parallel(src_path, dst_remote, workers=8):
    """
    Upload a file or directory to remote, preserving path structure.
    If directory, upload contents (files/subdirs) in parallel.

    Args:
        src_path (str or Path): Local file or directory path.
        dst_remote (str): Remote base path (e.g., 'remote:backup').
        workers (int): Number of parallel upload threads.
    """
    src_path = Path(src_path).resolve()
    if not src_path.exists():
        raise FileNotFoundError(f"Source {src_path} does not exist.")

    dst_path = f"{dst_remote.rstrip('/')}/{src_path.name}"

    if src_path.is_file():
        print(f"Uploading file: {src_path} -> {dst_path}")
        run_rclone(str(src_path), dst_path)
    else:
        print(f"Uploading directory contents in parallel: {src_path} -> {dst_path}")
        items = list(src_path.iterdir())
        if not items:
            print(f"Directory {src_path} is empty. Nothing to upload.")
            return

        def upload_item(item):
            item_dst = f"{dst_path.rstrip('/')}/{item.name}"
            print(f"Uploading {item} -> {item_dst}")
            run_rclone(str(item), item_dst)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            executor.map(upload_item, items)

def upload_file(src, dst):
    """Upload a single file."""
    run_rclone(src, dst)

def upload_files(src_list, dst):
    """Upload multiple files sequentially."""
    for src in src_list:
        run_rclone(src, dst)

def upload_directory(src, dst):
    """Upload an entire directory (and all its contents)."""
    src = Path(src).resolve()
    if not src.is_dir():
        raise NotADirectoryError(f"Source {src} is not a directory.")
    if not src.exists():
        raise FileNotFoundError(f"Source directory {src} does not exist.")
    run_rclone(str(src), dst)

def upload_files_parallel(src_list, dst, workers=8):
    """Upload multiple sources (files or dirs) in parallel."""
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for src in src_list:
            executor.submit(run_rclone, src, dst)

def download_file(src, dst):
    """Download a single file."""
    run_rclone(src, dst)

def download_files(src_list, dst):
    """Download multiple files sequentially."""
    for src in src_list:
        run_rclone(src, dst)

def download_directory(src, dst):
    """Download an entire directory from remote."""
    run_rclone(src, dst)

def download_files_parallel(src_list, dst, workers=8):
    """Download multiple sources in parallel."""
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for src in src_list:
            executor.submit(run_rclone, src, dst)