
import os 
from uRclone import upload_path_parallel

rem_dir = "/home/ljp238/OneDrive/ACODE/PexPurgatorySep25W1and2"
loc_dir = "/home/ljp238/Documents/PexPurgatorySep25W1and2/"

upload_path_parallel(src_path=loc_dir, dst_remote=rem_dir, workers=8)