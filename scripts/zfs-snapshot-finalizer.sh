#!/bin/bash
# zfs-snapshot-finalizer.sh - Manage ZFS snapshots and clean up clones

# Usage: ./zfs-snapshot-finalizer.sh <repo> <snapshot_id> <clone_dataset> <clone_mountpoint>
# Example: ./zfs-snapshot-finalizer.sh SV s1 zsd/SV-tmp-s1 /tmp/zsd-SV-s1

set -e

REPO=$1
SNAPSHOT_ID=$2
CLONE_DATASET=$3
CLONE_MOUNTPOINT=$4
ZFS_DATASET="zsd/${REPO}"
TEMP_SNAPSHOT="${SNAPSHOT_ID}-tmp-$(date +%s)"
TEMP_OLD="${SNAPSHOT_ID}-old-$(date +%s)"

# Validate inputs
if [ -z "$REPO" ] || [ -z "$SNAPSHOT_ID" ] || [ -z "$CLONE_DATASET" ] || [ -z "$CLONE_MOUNTPOINT" ]; then
    echo "Usage: ./zfs-snapshot-finalizer.sh <repo> <snapshot_id> <clone_dataset> <clone_mountpoint>" >&2
    exit 1
fi

# Verify the clone exists and is mounted
if ! sudo zfs list "$CLONE_DATASET" > /dev/null 2>&1; then
    echo "ERROR: Clone dataset ${CLONE_DATASET} does not exist" >&2
    exit 1
fi

if ! mountpoint -q "$CLONE_MOUNTPOINT"; then
    echo "ERROR: Clone mountpoint ${CLONE_MOUNTPOINT} is not mounted" >&2
    exit 1
fi

# Step 1: Copy modified data from clone to main dataset
echo "Syncing modified data from clone to main dataset..." >&2
sudo rsync -a --delete "${CLONE_MOUNTPOINT}/" "/var/repos/${ZFS_DATASET}/"

# Step 2: Create a temporary snapshot of the modified data
echo "Creating temporary snapshot: ${ZFS_DATASET}@${TEMP_SNAPSHOT}" >&2
sudo zfs snapshot "${ZFS_DATASET}@${TEMP_SNAPSHOT}"

# Step 3: Rename operations to replace the original snapshot
if sudo zfs list -t snapshot "${ZFS_DATASET}@${SNAPSHOT_ID}" > /dev/null 2>&1; then
    # Original snapshot exists, need to rename it first
    echo "Renaming original snapshot to temporary name..." >&2
    sudo zfs rename "${ZFS_DATASET}@${SNAPSHOT_ID}" "${ZFS_DATASET}@${TEMP_OLD}"
fi

# Step 4: Rename our new snapshot to the original name
echo "Renaming new snapshot to original name: ${ZFS_DATASET}@${SNAPSHOT_ID}" >&2
sudo zfs rename "${ZFS_DATASET}@${TEMP_SNAPSHOT}" "${ZFS_DATASET}@${SNAPSHOT_ID}"

# Step 5: Clean up the old snapshot if it exists
if sudo zfs list -t snapshot "${ZFS_DATASET}@${TEMP_OLD}" > /dev/null 2>&1; then
    echo "Removing old snapshot..." >&2
    # We need to destroy any dependent clones before we can destroy the snapshot
    # So we'll destroy the clone early
    echo "Destroying clone before removing old snapshot..." >&2
    
    # Unmount the clone
    if mountpoint -q "$CLONE_MOUNTPOINT"; then
        sudo zfs unmount "${CLONE_DATASET}" 2>/dev/null || true
    fi
    
    # Destroy the clone
    sudo zfs destroy -f "${CLONE_DATASET}" 2>/dev/null || true
    
    # Now try to destroy the old snapshot
    sudo zfs destroy "${ZFS_DATASET}@${TEMP_OLD}" 2>/dev/null || echo "Warning: Could not destroy old snapshot" >&2
fi

# Step 6: Clean up remaining resources
# We may have already destroyed the clone when removing the old snapshot
if sudo zfs list "${CLONE_DATASET}" > /dev/null 2>&1; then
    echo "Cleaning up: destroying remaining clone ${CLONE_DATASET}" >&2
    
    # Unmount the clone if still mounted
    if mountpoint -q "$CLONE_MOUNTPOINT"; then
        sudo zfs unmount "${CLONE_DATASET}" 2>/dev/null || echo "Warning: Failed to unmount clone" >&2
    fi
    
    # Destroy the clone
    sudo zfs destroy -f "${CLONE_DATASET}" 2>/dev/null || echo "Warning: Failed to destroy clone" >&2
fi

# Remove the mountpoint directory if it exists and is not mounted
if [ -d "$CLONE_MOUNTPOINT" ] && ! mountpoint -q "$CLONE_MOUNTPOINT"; then
    rmdir "$CLONE_MOUNTPOINT" 2>/dev/null || true
fi

echo "Successfully finalized snapshot ${ZFS_DATASET}@${SNAPSHOT_ID}" >&2