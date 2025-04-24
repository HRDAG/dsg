#!/usr/bin/env python3
#  noqa: E501
#  flake8: noqa: E501
#  pylint: disable=line-too-long

# call with for
"""Copy s* directories from BB to ZFS with sampled snapshot verification."""

import re
import random
import subprocess
import typer
import tempfile
from pathlib import Path
from loguru import logger
from contextlib import contextmanager

app = typer.Typer()
BTRSNAP_BASE = "/var/repos/btrsnap"
VERIFY_PROB = 0.25  # 25% chance to verify each snapshot


def get_sdir_numbers(bb_dir: str) -> list[int]:
    """Return sorted list of s directory numbers."""
    return sorted(
        int(d.name[1:]) for d in Path(bb_dir).iterdir()
        if d.is_dir() and re.match(r's\d+$', d.name)
    )


@contextmanager
def mount_snapshot(dataset: str, snapshot: str):
    """Context manager for temporary snapshot clone."""
    clone = None
    try:
        clone = f"{dataset}_verify_{snapshot.split('@')[-1]}"
        subprocess.run(["sudo", "zfs", "clone", f"{dataset}@{snapshot}", clone], check=True)
        subprocess.run(["sudo", "zfs", "set", "mountpoint=legacy", clone], check=True)

        with tempfile.TemporaryDirectory() as mountpoint:
            subprocess.run(["sudo", "mount", "-t", "zfs", clone, mountpoint], check=True)
            yield mountpoint
            subprocess.run(["sudo", "umount", mountpoint], check=True)
    finally:
        if clone:
            subprocess.run(["sudo", "zfs", "destroy", "-r", clone], stderr=subprocess.DEVNULL)


def verify_snapshot(bb_dir: str, dataset: str, num: int, verbose: bool) -> bool:
    """Verify snapshot matches source with probabilistic sampling."""
    try:
        with mount_snapshot(dataset, f"s{num}") as mountpoint:
            if verbose:
                logger.debug(f"Verifying s{num}:\nSource: {bb_dir}/s{num}\nSnapshot: {mountpoint}")
                subprocess.run(["ls", "-la", f"{bb_dir}/s{num}"])
                subprocess.run(["ls", "-la", mountpoint])

            result = subprocess.run(
                ["diff", "-rq", "--no-dereference", f"{bb_dir}/s{num}", mountpoint],
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                logger.warning(f"Snapshot s{num} verification failed!")
                # Run full diff if verification fails
                full_diff = subprocess.run(
                    ["diff", "-r", "--no-dereference", f"{bb_dir}/s{num}", mountpoint],
                    capture_output=True,
                    text=True
                )
                for line in full_diff.stdout.splitlines():
                    logger.warning(line)
                return False
            return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Verification error for s{num}: {e.stderr if e.stderr else str(e)}")
        return False


@app.command()
def main(
    bb: str = typer.Argument(..., help="BB directory name under /var/repos/btrsnap"),
    zfs_dataset: str = typer.Option("zsd", help="Base ZFS dataset path"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose debugging")
):
    """Copy s* directories with sampled snapshot verification."""
    try:
        bb_dir = f"{BTRSNAP_BASE}/{bb}"
        assert Path(bb_dir).exists(), f"Directory {bb_dir} does not exist"

        logger.add(f"/home/pball/tmp/log/btr-zfs-{bb}.log", level="DEBUG" if verbose else "INFO")
        full_dataset = f"{zfs_dataset}/{bb}"
        zfs_mount = f"/var/repos/{full_dataset}"

        # Create ZFS dataset if needed
        result = subprocess.run(["zfs", "list", full_dataset], capture_output=True)
        if result.returncode != 0:
            logger.info(f"Creating ZFS dataset {full_dataset}")
            subprocess.run(["zfs", "create", full_dataset], check=True)

        s_numbers = get_sdir_numbers(bb_dir)
        logger.info(f"Found {len(s_numbers)} s* directories in {bb_dir}")

        for num in s_numbers:
            src = f"{bb_dir}/s{num}/"
            logger.info(f"Processing {src}")

            # Rsync with delete for exact copy
            subprocess.run(["rsync", "-a", "--delete", src, zfs_mount], check=True)
            subprocess.run(["zfs", "snapshot", f"{full_dataset}@s{num}"], check=True)

            # Verify with 25% probability
            if random.random() < VERIFY_PROB or num == max(s_numbers):
                if verify_snapshot(bb_dir, full_dataset, num, verbose):
                    logger.info(f"Verified s{num} (sampled)")
                else:
                    logger.error(f"Snapshot verification failed for s{num} (sampled)")
                    raise typer.Exit(1)

        logger.success(f"Completed processing {len(s_numbers)} directories")
        logger.info(f"Verified {round(len(s_numbers)*VERIFY_PROB)} random snapshots")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e.stderr if e.stderr else str(e)}")
        raise typer.Exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
