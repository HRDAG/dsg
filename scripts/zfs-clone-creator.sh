#!/bin/bash
# zfs-clone-creator.sh - Create ZFS clones for metadata migration

# Usage: ./zfs-clone-creator.sh <repo> <snapshot_id>
# Example: ./zfs-clone-creator.sh SV s1
# Output: <clone_dataset>|<clone_mountpoint>

set -e

REPO=$1
SNAPSHOT_ID=$2
ZFS_DATASET="zsd/${REPO}"
CLONE_DATASET="zsd/${REPO}-tmp-${SNAPSHOT_ID}"
CLONE_MOUNTPOINT="/tmp/zsd-${REPO}-${SNAPSHOT_ID}"

# Validate inputs
if [ -z "$REPO" ] || [ -z "$SNAPSHOT_ID" ]; then
    echo "Usage: ./zfs-clone-creator.sh <repo> <snapshot_id>" >&2
    exit 1
fi

echo "Creating clone from ${ZFS_DATASET}@${SNAPSHOT_ID}" >&2

# Check if clone already exists
if sudo zfs list "$CLONE_DATASET" >/dev/null 2>&1; then
    # Check if it has snapshots and destroy them first
    if sudo zfs list -t snapshot -r "$CLONE_DATASET" >/dev/null 2>&1; then
        echo "Removing existing clone snapshots" >&2
        sudo zfs list -t snapshot -r "$CLONE_DATASET" -H -o name | sudo xargs -r zfs destroy 2>/dev/null || true
    fi
    
    # If it's already mounted, unmount it first
    if mountpoint -q "$CLONE_MOUNTPOINT"; then
        echo "Unmounting existing clone" >&2
        sudo zfs unmount "$CLONE_DATASET" 2>/dev/null || true
    fi
    
    # Then destroy it
    echo "Destroying existing clone" >&2
    sudo zfs destroy -f "$CLONE_DATASET" 2>/dev/null || true
fi

# Verify that the source snapshot exists
if ! sudo zfs list -t snapshot "${ZFS_DATASET}@${SNAPSHOT_ID}" >/dev/null 2>&1; then
    echo "Error: Source snapshot ${ZFS_DATASET}@${SNAPSHOT_ID} does not exist!" >&2
    exit 1
fi

# Create the clone
echo "Creating new clone from ${ZFS_DATASET}@${SNAPSHOT_ID}" >&2
sudo zfs clone "${ZFS_DATASET}@${SNAPSHOT_ID}" "$CLONE_DATASET"

# Clean up old mountpoint if it exists
if [ -d "$CLONE_MOUNTPOINT" ]; then
    if mountpoint -q "$CLONE_MOUNTPOINT"; then
        sudo umount "$CLONE_MOUNTPOINT" 2>/dev/null || true
    fi
    rm -rf "$CLONE_MOUNTPOINT" 2>/dev/null || true
fi

# Create the mountpoint directory
mkdir -p "$CLONE_MOUNTPOINT"

# Set mountpoint and mount the clone
sudo zfs set "mountpoint=${CLONE_MOUNTPOINT}" "$CLONE_DATASET"
sudo zfs mount "$CLONE_DATASET" 2>/dev/null || true

# Verify clone is mounted
if ! mountpoint -q "$CLONE_MOUNTPOINT"; then
    echo "Failed to mount ${CLONE_DATASET} at ${CLONE_MOUNTPOINT}" >&2
    sudo zfs destroy -f "$CLONE_DATASET" 2>/dev/null || true
    exit 1
fi

echo "Successfully created and mounted clone at ${CLONE_MOUNTPOINT}" >&2

# Output the clone dataset and mountpoint for piping to next script
echo "${CLONE_DATASET}|${CLONE_MOUNTPOINT}"