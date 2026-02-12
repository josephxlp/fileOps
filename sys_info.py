import subprocess
import re
import psutil
import shutil

def get_system_info():
    info = {}

    # CPU
    try:
        with open("/proc/cpuinfo", "r") as f:
            cpuinfo = f.read()
        match = re.search(r"model name\s+:\s+(.+)", cpuinfo)
        info["cpu"] = match.group(1).strip() if match else "Unknown CPU"
    except Exception as e:
        info["cpu"] = f"Error reading CPU info: {e}"

    # RAM
    try:
        mem = psutil.virtual_memory()
        info["ram_gb"] = round(mem.total / (1024**3))
    except Exception as e:
        info["ram_gb"] = f"Error reading RAM: {e}"

    # GPU and CUDA
    if shutil.which("nvidia-smi"):
        try:
            # Get full nvidia-smi output
            result = subprocess.run(
                ["nvidia-smi"],
                capture_output=True,
                text=True,
                check=True
            )
            smi_output = result.stdout

            # Extract GPU name(s)
            gpu_names = re.findall(r"\|\s+\d+\s+NVIDIA\s+(.+?)\s+\w+\s+\w+\s+\w+", smi_output)
            if gpu_names:
                info["gpu"] = ", ".join(gpu_names)
            else:
                info["gpu"] = "NVIDIA GPU detected (name parsing failed)"

            # Extract CUDA version from header (e.g., "CUDA Version: 12.4")
            cuda_match = re.search(r"CUDA Version:\s*([\d\.]+)", smi_output)
            if cuda_match:
                info["cuda_version"] = cuda_match.group(1)
            else:
                info["cuda_version"] = "Not reported by nvidia-smi"

            # Extract driver version (optional)
            driver_match = re.search(r"Driver Version:\s*([\d\.]+)", smi_output)
            if driver_match:
                info["nvidia_driver"] = driver_match.group(1)

        except Exception as e:
            info["gpu"] = f"Error running nvidia-smi: {e}"
            info["cuda_version"] = "N/A"
    else:
        info["gpu"] = "NVIDIA GPU not detected or nvidia-smi not installed"
        info["cuda_version"] = "N/A"

    return info

if __name__ == "__main__":
    system_info = get_system_info()
    print("System Information:")
    for key, value in system_info.items():
        print(f"{key.replace('_', ' ').title()}: {value}")