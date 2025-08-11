import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

def get_directory_size(path):
    """
    Recursively calculates the total size of a directory.

    Args:
        path (str): Path to the directory.

    Returns:
        int: Total size in bytes.
    """
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for file in filenames:
                filepath = os.path.join(dirpath, file)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception as e:
        print(f"Error calculating size for {path}: {e}")
    return total_size

def send_notification(message):
    """
    Sends a desktop notification using notify-send (Linux).

    Args:
        message (str): Notification message.

    Returns:
        None
    """
    try:
        os.system(f'notify-send "Trash Cleanup" "{message}"')
    except Exception as e:
        print(f"Failed to send notification: {e}")

def clean_trash(trash_path, level, verbose=False):
    """
    Cleans the trash directory based on the specified level.

    Args:
        trash_path (str): Path to the trash directory (e.g., /media/user/.Trash-1000).
        level (int): Cleaning level:
            - 1: Deletes all contents in the 'files' directory.
            - 2: Deletes all contents in the 'expunged' directory.
            - 3: Deletes all .trashinfo files in the 'info' directory.
            - 4: Deletes everything in 'files', 'expunged', and 'info'.
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

    # Validate input level
    if level not in [1, 2, 3, 4]:
        raise ValueError("Invalid level. Must be 1, 2, 3, or 4.")

    # Define target directories based on level
    if level == 1:
        target_dirs = [os.path.join(trash_path, "files")]
    elif level == 2:
        target_dirs = [
            os.path.join(trash_path, "files"),
            os.path.join(trash_path, "expunged"),
            #os.path.join(trash_path, "info")
        ]
    elif level == 3:
        #target_dirs = [os.path.join(trash_path, "info")]
        target_dirs = [
            os.path.join(trash_path, "files"),
            os.path.join(trash_path, "expunged"),
            os.path.join(trash_path, "info")
        ]
    else:
        print('Level does not exist...')

    # Calculate initial trash size
    start_time = time.time()
    initial_size = sum(get_directory_size(d) for d in target_dirs if os.path.exists(d))
    initial_size_gb = initial_size / (1024 ** 3)  # Convert to GB

    # Collect all paths to delete
    paths_to_delete = []
    for target_dir in target_dirs:
        if not os.path.exists(target_dir):
            print(f"Directory {target_dir} does not exist. Skipping.")
            continue

        for root, dirs, files in os.walk(target_dir):
            for name in files:
                paths_to_delete.append(os.path.join(root, name))
            for name in dirs:
                paths_to_delete.append(os.path.join(root, name))

        # Special case for level 3: Only delete .trashinfo files
        if level == 3:
            paths_to_delete = [p for p in paths_to_delete if p.endswith(".trashinfo")]

    # Use parallel processing to delete paths
    with ThreadPoolExecutor() as executor:
        executor.map(delete_path, paths_to_delete)

    # Calculate final trash size
    final_size = sum(get_directory_size(d) for d in target_dirs if os.path.exists(d))
    final_size_gb = final_size / (1024 ** 3)  # Convert to GB

    # Measure execution time
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Print results
    print(f"Trash cleaning completed for level {level}.")
    print(f"Initial trash size: {initial_size_gb:.2f} GB")
    print(f"Final trash size: {final_size_gb:.2f} GB")
    print(f"Time taken: {elapsed_time:.2f} seconds")

    # Send notification
    notification_message = (
        f"Level {level} cleanup complete.\n"
        f"Initial size: {initial_size_gb:.2f} GB\n"
        f"Final size: {final_size_gb:.2f} GB\n"
        f"Time taken: {elapsed_time:.2f} seconds"
    )
    send_notification(notification_message)

# Example usage

drive_name = None
drive_name = "driveb" 
drive_name = "drivea" 
level = 2


drive_names = ["drivea","driveb",None]
local_trash = "/home/ljp238/.local/share/Trash"

if __name__ == "__main__":
    from upaths import name
    external1_trash = f"/media/{name}/12TBWolf/.Trash-1001" # is there a .Trans-1001 as well?
    clean_trash(local_trash,level=level, verbose=True)
    clean_trash(external1_trash,level=level, verbose=True)
 
