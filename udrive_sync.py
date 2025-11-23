#!/usr/bin/env python3
"""
drive_sync.py

End-to-end, SSD/HDD-aware, rsync-accelerated directory copy tool.

Features:
- Auto-detects whether source/destination drives are SSD or HDD
- Picks rsync + system tuning flags accordingly
- Uses ionice/nice for I/O/CPU priority
- Optionally runs parallel rsync workers for SSD->SSD (auto-splits top-level subdirs)
- Pre/post size + file-count checks to detect already-copied state
- Timing, logging, and desktop notifications (notify-send)
- Robust retry logic

Usage:
    ./drive_sync.py /path/to/source /path/to/destination

Run --help for options.
"""
import argparse
import os
import subprocess
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# -------------------------
# Configuration defaults
# -------------------------
DEFAULT_LOG = str(Path.home() / ".drive_sync.log")
DEFAULT_MODE = "fast"  # safe | fast | aggressive
DEFAULT_PARALLEL_WORKERS = 4
RSYNC_BIN = "rsync"
NOTIFY_BIN = "notify-send"
FIND_BIN = "find"
DU_BIN = "du"

# -------------------------
# Logging setup
# -------------------------
logger = logging.getLogger("drive_sync")


def setup_logging(logfile, verbose=False):
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)


# -------------------------
# Notification
# -------------------------
def send_notification(summary: str, body: str = ""):
    try:
        subprocess.run([NOTIFY_BIN, summary, body], check=False)
    except Exception:
        logger.debug("notify-send failed or not available.")


# -------------------------
# Helpers: size and file count
# -------------------------
def get_size_and_count(path: str):
    """
    Efficiently compute total size (bytes) and file count using system tools.
    Falls back to Python walk if system tools fail.
    """
    path = str(path)
    # Prefer du -sb for bytes and find for file count
    try:
        du_out = subprocess.check_output([DU_BIN, "-sb", path], stderr=subprocess.DEVNULL)
        size = int(du_out.split()[0])
    except Exception:
        logger.debug("du failed; falling back to os.walk for size")
        size = 0
        for root, dirs, files in os.walk(path):
            for f in files:
                try:
                    size += os.path.getsize(os.path.join(root, f))
                except Exception:
                    pass

    try:
        find_cmd = f"{FIND_BIN} '{path}' -type f | wc -l"
        count = int(subprocess.check_output(["bash", "-c", find_cmd]).strip())
    except Exception:
        logger.debug("find failed; falling back to os.walk for count")
        count = 0
        for _, _, files in os.walk(path):
            count += len(files)

    return size, count


# -------------------------
# Detect device and rotational flag
# -------------------------
def _device_for_path(path: str):
    """Return the block device (e.g., /dev/sda1) that contains path using df."""
    try:
        out = subprocess.check_output(["df", "--output=source", path], stderr=subprocess.DEVNULL).decode().splitlines()
        # output: header line + device line
        if len(out) >= 2:
            device = out[1].strip()
            return device
    except Exception:
        logger.debug("df failed to map device for path")
    return None


def _block_name_from_device(device: str):
    """
    Convert device like /dev/sda1 or /dev/nvme0n1p1 to block base name (sda or nvme0n1).
    We attempt to strip partition suffixes.
    """
    if not device:
        return None
    base = os.path.basename(device)
    # handle nvme style partitions (nvme0n1p1 -> nvme0n1)
    if base.startswith("nvme") and "p" in base:
        # remove trailing p<digits>
        return base.split("p")[0]
    # strip trailing digits for typical devices (sda1 -> sda)
    # but careful: mmcblk0p1 -> mmcblk0
    # common rule: strip trailing digits, but if ends with digit preceding 'p', handle above
    # try reading /sys/class/block for exact match
    try:
        candidates = os.listdir("/sys/class/block")
        if base in candidates:
            return base
        # if base not present, try removing digits
        import re

        m = re.match(r"^([a-zA-Z]+)", base)
        if m:
            prefix = m.group(1)
            if prefix in candidates:
                return prefix
    except Exception:
        pass
    # fallback
    return base.rstrip("0123456789")


def is_rotational(block_name: str):
    """Return True if rotational=1 (HDD), False if 0 (SSD)."""
    if not block_name:
        return False
    path = f"/sys/block/{block_name}/queue/rotational"
    try:
        with open(path, "r") as fh:
            val = fh.read().strip()
            return val == "1"
    except Exception:
        # fallback: assume SSD for safety (so we prefer --whole-file on SSD)
        logger.debug(f"Could not read rotational flag at {path}; assuming SSD")
        return False


def is_ssd_for_path(path: str):
    """Detect whether the filesystem containing `path` is SSD (True) or HDD (False)."""
    device = _device_for_path(path)
    if not device:
        return False
    block = _block_name_from_device(device)
    return not is_rotational(block)


# -------------------------
# Build rsync command depending on mode and device types
# -------------------------
def build_rsync_cmd(src: str, dst: str, src_is_ssd: bool, dst_is_ssd: bool, mode: str = "fast"):
    src = src.rstrip("/") + "/"
    dst = dst.rstrip("/") + "/"

    # base safe flags
    flags = ["-a", "-H", "-A", "-X", "--numeric-ids"]  # archive + preserve hardlinks, acls, xattrs, numeric ids

    # progress
    flags.append("--info=progress2")

    # tuning by mode and device types
    pre_cmd = []  # will hold ionice/nice if needed

    if mode == "safe":
        # conservative
        flags += ["--partial", "--inplace"]
        # no nice or ionice changes by default in safe mode
    elif mode == "fast":
        flags += ["--partial", "--inplace"]
        # prefer whole-file when copying SSD -> SSD (faster)
        if src_is_ssd and dst_is_ssd:
            flags += ["--whole-file", "--no-inc-recursive"]
        else:
            # on HDDs, allow delta algorithm (default), but --no-inc-recursive can help on large sets
            flags += ["--no-inc-recursive"]
        # Add ionice and nice (best-effort high priority)
        pre_cmd += ["ionice", "-c2", "-n0", "nice", "-n", "-5"]
    elif mode == "aggressive":
        # for users who requested top-speed regardless of system impact
        flags += ["--partial", "--inplace", "--no-inc-recursive", "--whole-file"]
        pre_cmd += ["ionice", "-c1", "-n0", "nice", "-n", "-10"]
    else:
        # default to fast
        return build_rsync_cmd(src, dst, src_is_ssd, dst_is_ssd, mode="fast")

    rsync_cmd = pre_cmd + [RSYNC_BIN] + flags + [src, dst]
    return rsync_cmd


# -------------------------
# Run rsync with live stdout streaming
# -------------------------
def run_rsync_stream(cmd):
    logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

    # stream stdout
    try:
        for line in proc.stdout:
            print(line, end="")
            logger.debug(line.rstrip())
    except Exception as e:
        logger.debug("Error streaming rsync stdout: %s", e)

    # capture remaining stderr after stdout loop
    stderr = proc.stderr.read() if proc.stderr else ""
    proc.wait()
    ret = proc.returncode
    if stderr:
        for line in stderr.splitlines():
            logger.debug("[rsync-stderr] %s", line)
    logger.info("rsync exit code: %s", ret)
    return ret


# -------------------------
# Parallel rsync by top-level dirs (SSD->SSD)
# -------------------------
def list_top_level_entries(path: str):
    """
    Return a list of top-level subpaths (files and dirs) inside path.
    We skip hidden entries optionally (here we include everything).
    """
    p = Path(path)
    entries = [str(p / e) for e in os.listdir(p)]
    # Filter out '.' and '..' if present
    entries = [e for e in entries if not os.path.basename(e) in (".", "..")]
    return entries


def parallel_rsync_split(src: str, dst: str, workers: int, base_cmd_builder, src_is_ssd, dst_is_ssd, mode):
    """
    Splits top-level entries into worker tasks. Each worker runs rsync for its subset.
    This is safe when src contains many top-level directories and the dest is empty or same structure.
    """
    logger.info("Using parallel rsync with %d workers", workers)
    entries = list_top_level_entries(src)
    if not entries:
        # nothing to do
        logger.info("No top-level entries to parallelize.")
        return 0

    # split entries into worker lists
    chunks = [[] for _ in range(workers)]
    for idx, entry in enumerate(entries):
        chunks[idx % workers].append(entry)

    futures = []
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, chunk in enumerate(chunks):
            if not chunk:
                continue
            # build an rsync invocation that syncs this chunk into dst/<basename> (preserving names)
            # We'll rsync each chunk's items to dst/ using rsync's ability to take multiple source args.
            src_args = chunk
            # build flags using base builder, but we need to place the src args before dst
            base_cmd = build_rsync_cmd(src, dst, src_is_ssd, dst_is_ssd, mode)
            # base_cmd looks like [ionice..., rsync, flags..., src/, dst/]
            # we will transform to: pre_cmd + [rsync] + flags + src1 src2 ... dst/
            # find index of RSYNC_BIN
            try:
                rsync_index = base_cmd.index(RSYNC_BIN)
            except ValueError:
                rsync_index = 0
            pre = base_cmd[:rsync_index]
            post = base_cmd[rsync_index + 1:]  # flags + src + dst
            # remove original src and dst from post
            # we assume last two items are src and dst
            if len(post) >= 2:
                flags = post[:-2]
            else:
                flags = post
            cmd = pre + [RSYNC_BIN] + flags + src_args + [dst.rstrip("/") + "/"]
            logger.debug("Worker %d cmd: %s", i, " ".join(cmd))
            futures.append(ex.submit(run_rsync_stream, cmd))

        for f in as_completed(futures):
            results.append(f.result())

    # return max exit code (0 success)
    return max(results) if results else 0


# -------------------------
# Top-level orchestration
# -------------------------
def sync_directories(src: str, dst: str, mode: str = "fast", parallel_workers=DEFAULT_PARALLEL_WORKERS, verbose=False):
    start_time = time.time()
    logger.info("Starting sync: '%s' -> '%s' (mode=%s)", src, dst, mode)
    send_notification("Drive Sync", f"Starting sync: {os.path.basename(src)} -> {os.path.basename(dst)}")

    # check existence
    if not os.path.exists(src):
        logger.error("Source does not exist: %s", src)
        send_notification("Drive Sync Error", f"Source missing: {src}")
        raise FileNotFoundError(src)
    os.makedirs(dst, exist_ok=True)

    # perform pre-check: sizes and counts
    logger.info("Gathering size and file count for source...")
    src_size, src_count = get_size_and_count(src)
    logger.info("Source size: %.3f GB, files: %d", src_size / 1e9, src_count)

    logger.info("Gathering size and file count for destination (if exists)...")
    dst_size, dst_count = get_size_and_count(dst)
    logger.info("Dest size: %.3f GB, files: %d", dst_size / 1e9, dst_count)

    if src_size == dst_size and src_count == dst_count and src_size != 0:
        logger.info("Source and destination appear identical (size & count). Skipping rsync.")
        send_notification("Drive Sync", "Already synced: no action required.")
        elapsed = time.time() - start_time
        logger.info("Elapsed: %.2f s", elapsed)
        return 0

    # Detect SSD/HDD status
    try:
        src_ssd = is_ssd_for_path(src)
        dst_ssd = is_ssd_for_path(dst)
    except Exception as e:
        logger.debug("SSD detection failed (%s); assuming SSD for speed", e)
        src_ssd = dst_ssd = True

    logger.info("Source is %s", "SSD" if src_ssd else "HDD")
    logger.info("Destination is %s", "SSD" if dst_ssd else "HDD")

    # Build main command or choose parallel strategy
    if src_ssd and dst_ssd and parallel_workers and parallel_workers > 1:
        # Use parallel split approach
        logger.info("SSD->SSD detected: enabling parallel rsync with up to %d workers", parallel_workers)
        ret = parallel_rsync_split(src, dst, parallel_workers, build_rsync_cmd, src_ssd, dst_ssd, mode)
    else:
        # Single rsync
        cmd = build_rsync_cmd(src, dst, src_ssd, dst_ssd, mode)
        ret = run_rsync_stream(cmd)

    if ret != 0:
        logger.warning("rsync reported non-zero exit: %s. Will attempt a second pass.", ret)
        # retry once more single-threaded
        cmd = build_rsync_cmd(src, dst, src_ssd, dst_ssd, mode)
        ret = run_rsync_stream(cmd)

    # final verification
    final_src_size, final_src_count = get_size_and_count(src)
    final_dst_size, final_dst_count = get_size_and_count(dst)

    if final_src_size == final_dst_size and final_src_count == final_dst_count and final_src_size != 0:
        elapsed = time.time() - start_time
        logger.info("Transfer verified OK. Time: %s", format_elapsed(elapsed))
        send_notification("Drive Sync: Completed", f"Time: {format_elapsed(elapsed)}")
        return 0
    else:
        logger.error("Mismatch after rsync. src (size/files) = %s/%s ; dst = %s/%s",
                     final_src_size, final_src_count, final_dst_size, final_dst_count)
        send_notification("Drive Sync: ERROR", "Size/file count mismatch after rsync.")
        return 2


def format_elapsed(seconds):
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    s = s % 60
    return f"{h}h {m}m {s}s"


# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(description="SSD/HDD-aware rsync-based directory sync")
    p.add_argument("source", help="Source directory (must exist)")
    p.add_argument("destination", help="Destination directory (will be created if missing)")
    p.add_argument("--mode", choices=("safe", "fast", "aggressive"), default=DEFAULT_MODE,
                   help="Aggressiveness mode (defaults to 'fast')")
    p.add_argument("--workers", type=int, default=DEFAULT_PARALLEL_WORKERS,
                   help="Parallel workers for SSD->SSD (default: 4). Set 0 or 1 to disable.")
    p.add_argument("--log", default=DEFAULT_LOG, help=f"Log file (default: {DEFAULT_LOG})")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose stdout logging")
    return p.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log, verbose=args.verbose)
    logger.info("drive_sync started")
    try:
        rc = sync_directories(args.source, args.destination, mode=args.mode,
                              parallel_workers=args.workers, verbose=args.verbose)
        logger.info("drive_sync finished with code %s", rc)
        sys.exit(rc)
    except Exception as e:
        logger.exception("Fatal error during sync: %s", e)
        send_notification("Drive Sync: Fatal error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
