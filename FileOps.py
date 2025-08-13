import os 
import subprocess 


def unzip_fast(zip_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    subprocess.run(["unzip", "-q", zip_path, "-d", output_dir], check=True)



    