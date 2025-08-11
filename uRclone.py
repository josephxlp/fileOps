import subprocess
from concurrent.futures import ThreadPoolExecutor

def run_rclone(src, dst):
    subprocess.run(["rclone", "copy", src, dst, "--progress"], check=True)

def upload_file(src, dst):
    run_rclone(src, dst)

def upload_files(src_list, dst):
    for src in src_list:
        run_rclone(src, dst)

def upload_dir(src, dst):
    run_rclone(src, dst)

def upload_files_parallel(src_list, dst, workers=8):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for src in src_list:
            executor.submit(run_rclone, src, dst)

def download_file(src, dst):
    run_rclone(src, dst)

def download_files(src_list, dst):
    for src in src_list:
        run_rclone(src, dst)

def download_dir(src, dst):
    run_rclone(src, dst)

def download_files_parallel(src_list, dst, workers=8):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for src in src_list:
            executor.submit(run_rclone, src, dst)
