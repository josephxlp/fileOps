import shutil
import os
import glob
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def copy_file(src, dst, progress):
    """Copy a single file from src to dst."""
    try:
        shutil.copy2(src, dst)
        progress.update(1)
        print(f"Copied: {src} -> {dst}")
    except Exception as e:
        print(f"Error copying {src}: {e}")

def copy_files_parallel(file_list, dst_dir, max_workers=4):
    """
    Copy multiple files in parallel to the destination directory.
    
    :param file_list: List of file paths to copy.
    :param dst_dir: Destination directory where files will be copied.
    :param max_workers: Number of parallel threads to use.
    """
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
    
    with tqdm(total=len(file_list), desc="Copying Files") as progress:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for src in file_list:
                dst = os.path.join(dst_dir, os.path.basename(src))
                futures.append(executor.submit(copy_file, src, dst, progress))
            
            # Ensure all tasks are completed
            for future in futures:
                future.result()
onedrive_dpath = "/home/ljp238/OneDrive/PDATA/GEDI_parquet/"
storage_dpath = "/media/ljp238/12TBWolf/ARCHIEVE/GEDI/PARQUET/"
files = glob.glob(f"{onedrive_dpath}/*.gz"); print(len(files))
copy_files_parallel(files,storage_dpath, max_workers=20)#
# is this slow?



