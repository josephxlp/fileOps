import os
import tarfile
import shutil
from concurrent.futures import ThreadPoolExecutor
import os
import zipfile
import tarfile
import concurrent.futures
from datetime import datetime

def unzip_file(zip_path, extract_to):
    """Extracts a ZIP file into a specific directory."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted: {zip_path} → {extract_to}")
    except Exception as e:
        print(f"Error extracting {zip_path}: {e}")

def extract_tar_file(tar_path, extract_to):
    """Extracts a TAR, TAR.GZ, or TAR.BZ2 file into a specific directory."""
    try:
        with tarfile.open(tar_path, 'r:*') as tar_ref:
            tar_ref.extractall(extract_to)
        print(f"Extracted: {tar_path} → {extract_to}")
    except Exception as e:
        print(f"Error extracting {tar_path}: {e}")

def extract_file(file_path, destination_root):
    """Creates a folder named after the archive (without extension) and extracts there."""
    file_name = os.path.basename(file_path)
    folder_name = os.path.splitext(file_name)[0]  # Remove extension
    extract_to = os.path.join(destination_root, folder_name)

    # Ensure the extraction directory exists
    os.makedirs(extract_to, exist_ok=True)

    if file_path.endswith('.zip'):
        unzip_file(file_path, extract_to)
    elif file_path.endswith(('.tar', '.tar.gz', '.tar.bz2')):
        extract_tar_file(file_path, extract_to)

    send_notification(message=f"Script execution completed \n{folder_name}", duration=5000)

def extract_files_in_directory(directory, extract_to, num_workers=8):
    """Extracts all compressed files in a directory using parallel threads."""
    files_to_extract = [
        os.path.join(directory, f) for f in os.listdir(directory)
        if f.endswith(('.zip', '.tar', '.tar.gz', '.tar.bz2'))
    ]

    if not files_to_extract:
        print("No compressed files found.")
        return

    print(f"Found {len(files_to_extract)} files. Extracting with {num_workers} workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(lambda f: extract_file(f, extract_to), files_to_extract)


def extract_tarball(source, destination):
    if not os.path.exists(source):
        print(f"Error: {source} does not exist.")
        return
    os.makedirs(destination, exist_ok=True)
    try:
        with tarfile.open(source, "r:*") as tar:
            tar.extractall(path=destination)
        print(f"Extracted {source} to {destination}")
    except Exception as e:
        print(f"Error extracting tarball: {e}")

def create_tarball(source, destination):
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    try:
        with tarfile.open(destination, "w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))
        print(f"Tarball created at {destination}")
    except Exception as e:
        print(f"Error creating tarball: {e}")

def delete_folder_contents(folder_path, verbose=False):
    """
    Deletes all contents (files, subdirectories, and hidden files) inside a given folder.

    Args:
        folder_path (str): Path to the folder whose contents need to be deleted.
        verbose (bool): If True, prints the names of files/folders being removed.

    Returns:
        None
    """
    def delete_path(path):
        """Deletes a file or directory recursively."""
        try:
            if os.path.isfile(path) or os.path.islink(path):
                if verbose:
                    print(f"Deleting file: {path}")
                os.unlink(path)  # Remove file or symbolic link
            elif os.path.isdir(path):
                if verbose:
                    print(f"Deleting folder: {path}")
                shutil.rmtree(path)  # Remove directory and its contents
        except Exception as e:
            print(f"Error deleting {path}: {e}")

    # Validate folder path
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder '{folder_path}' does not exist.")
    if not os.path.isdir(folder_path):
        raise ValueError(f"'{folder_path}' is not a valid directory.")

    # Collect all paths to delete
    paths_to_delete = []
    for root, dirs, files in os.walk(folder_path):
        for name in files:
            paths_to_delete.append(os.path.join(root, name))
        for name in dirs:
            paths_to_delete.append(os.path.join(root, name))

    # Use parallel processing to delete paths
    with ThreadPoolExecutor() as executor:
        executor.map(delete_path, paths_to_delete)

    print(f"All contents of folder '{folder_path}' have been deleted.")

def send_notification(message="Script execution completed", duration=5000):
    """Sends a desktop notification (Linux only)."""
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.system(f'notify-send -u normal -t {duration} "{message} at {time_now}"')

    