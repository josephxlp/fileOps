
from fileops import send_notification, delete_folder_contents


if __name__ == "__main__":
    send_notification()
    folder_to_clean = "/media/ljp238/12TBWolf/ARXIV1/GEEDataDownload/" #"  # Replace with your folder path
    folder_to_clean = "/media/ljp238/12TBWolf/ARXIV1/SENTINEL1_DRFA/"
    folder_to_clean = source = "/media/ljp238/12TBWolf/SPPHDU3/"
    folder_to_clean = "/media/ljp238/12TBWolf/ARXIV2/TILESV2/genRON/clips     "
    delete_folder_contents(folder_to_clean, verbose=True)
    send_notification()