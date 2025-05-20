#!/bin/bash
# test-zfs-scripts.sh - Test ZFS clone creator and finalizer scripts

# Usage: ./test-zfs-scripts.sh <repo> <snapshot_id>
# Example: ./test-zfs-scripts.sh SV s1

set -e

REPO=$1
SNAPSHOT_ID=$2
TEST_FILE_NAME="zfs_script_test_$(date +%s).txt"
TEST_FILE_CONTENT="This is a test file created at $(date) to verify ZFS scripts functionality"
BACKUP_SNAPSHOT="pre-script-test-20250519-180155"

# Validate inputs
if [ -z "$REPO" ] || [ -z "$SNAPSHOT_ID" ]; then
    echo "Usage: ./test-zfs-scripts.sh <repo> <snapshot_id>"
    exit 1
fi

# Make scripts executable
chmod +x zfs-clone-creator.sh
chmod +x zfs-snapshot-finalizer.sh

echo "========================================================"
echo "  Testing ZFS Scripts with $REPO/$SNAPSHOT_ID"
echo "========================================================"
echo ""

# Clean up any existing test artifacts from previous runs
echo "Cleaning up any leftover artifacts from previous tests..."
# Clean up any verification mountpoint
if mountpoint -q "/tmp/verify-${REPO}-${SNAPSHOT_ID}"; then
    sudo umount "/tmp/verify-${REPO}-${SNAPSHOT_ID}" 2>/dev/null || true
fi
rm -rf "/tmp/verify-${REPO}-${SNAPSHOT_ID}" 2>/dev/null || true

# Step 1: Verify that the snapshot exists
echo "Verifying snapshot exists..."
if ! sudo zfs list -t snapshot "zsd/${REPO}@${SNAPSHOT_ID}" > /dev/null 2>&1; then
    echo "ERROR: Snapshot zsd/${REPO}@${SNAPSHOT_ID} does not exist!"
    exit 1
fi
echo "Snapshot exists. Proceeding with test."
echo ""

# Step 2: Create a clone
echo "Step 1: Creating ZFS clone..."
CLONE_INFO=$(./zfs-clone-creator.sh "$REPO" "$SNAPSHOT_ID")
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create clone!"
    exit 1
fi

# Parse clone info
IFS='|' read -r CLONE_DATASET CLONE_MOUNTPOINT <<< "$CLONE_INFO"
echo "Clone created successfully:"
echo "   Dataset: $CLONE_DATASET"
echo "   Mountpoint: $CLONE_MOUNTPOINT"
echo ""

# Step 3: Create a test file in the clone
echo "Step 2: Creating test file in clone..."
TEST_FILE_PATH="$CLONE_MOUNTPOINT/$TEST_FILE_NAME"
echo "$TEST_FILE_CONTENT" | sudo tee "$TEST_FILE_PATH" > /dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create test file!"
    # Cleanup
    sudo zfs destroy -f "$CLONE_DATASET" 2>/dev/null || true
    exit 1
fi
echo "Test file created: $TEST_FILE_PATH"
echo "Content: '$TEST_FILE_CONTENT'"
echo ""

# Save the original list of snapshots for comparison
echo "Saving list of current snapshots for verification..."
BEFORE_SNAPSHOTS=$(sudo zfs list -t snapshot | grep "zsd/${REPO}@" | wc -l)
echo "Current snapshots: $BEFORE_SNAPSHOTS"
echo ""

# Step 4: Finalize the snapshot
echo "Step 3: Finalizing snapshot..."
./zfs-snapshot-finalizer.sh "$REPO" "$SNAPSHOT_ID" "$CLONE_DATASET" "$CLONE_MOUNTPOINT"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to finalize snapshot!"
    exit 1
fi
echo "Snapshot finalized successfully."
echo ""

# Check if the number of snapshots remains the same (replacement was done correctly)
AFTER_SNAPSHOTS=$(sudo zfs list -t snapshot | grep "zsd/${REPO}@" | wc -l)
echo "Snapshots after finalization: $AFTER_SNAPSHOTS"
if [ "$BEFORE_SNAPSHOTS" -eq "$AFTER_SNAPSHOTS" ]; then
    echo "Snapshot count verification: PASSED (replacement was clean)"
else
    echo "Snapshot count verification: FAILED (extra snapshots were created)"
    echo "Before: $BEFORE_SNAPSHOTS, After: $AFTER_SNAPSHOTS"
fi
echo ""

# Step 5: Verify test file exists in the snapshot
echo "Step 4: Verifying test file in snapshot..."
SNAPSHOT_MOUNT="/tmp/verify-${REPO}-${SNAPSHOT_ID}"

# Clean up any existing mountpoint
if mountpoint -q "$SNAPSHOT_MOUNT"; then
    sudo umount "$SNAPSHOT_MOUNT" || true
fi
rm -rf "$SNAPSHOT_MOUNT"
mkdir -p "$SNAPSHOT_MOUNT"

# Mount the snapshot - using bind mount to access the snapshot
echo "Mounting snapshot for verification (using bind mount)..."
SNAPSHOT_PATH="/var/repos/zsd/${REPO}/.zfs/snapshot/${SNAPSHOT_ID}"
if [ ! -d "$SNAPSHOT_PATH" ]; then
    echo "ERROR: Snapshot path $SNAPSHOT_PATH does not exist!"
    rmdir "$SNAPSHOT_MOUNT"
    exit 1
fi

# Use bind mount instead of direct ZFS mount
sudo mount --bind "$SNAPSHOT_PATH" "$SNAPSHOT_MOUNT"
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to mount snapshot for verification!"
    rmdir "$SNAPSHOT_MOUNT"
    exit 1
fi

# Check if test file exists
if [ -f "$SNAPSHOT_MOUNT/$TEST_FILE_NAME" ]; then
    CONTENT=$(cat "$SNAPSHOT_MOUNT/$TEST_FILE_NAME")
    echo "SUCCESS: Test file found in snapshot!"
    echo "Content: '$CONTENT'"
    if [ "$CONTENT" == "$TEST_FILE_CONTENT" ]; then
        echo "Content verification: PASSED"
    else
        echo "Content verification: FAILED"
    fi
else
    echo "ERROR: Test file not found in snapshot!"
    # Unmount and exit
    sudo umount "$SNAPSHOT_MOUNT"
    rmdir "$SNAPSHOT_MOUNT"
    exit 1
fi

# Unmount verification snapshot
sudo umount "$SNAPSHOT_MOUNT"
rmdir "$SNAPSHOT_MOUNT"
echo ""

# Step 6: Report success
echo "========================================================"
echo "  ZFS Scripts Test: SUCCESS"
echo "========================================================"
echo "The zfs-clone-creator.sh and zfs-snapshot-finalizer.sh scripts"
echo "are working correctly for modifying ZFS snapshots!"
echo ""

# Step 7: Prompt for rollback
echo "Would you like to roll back to the backup snapshot?"
echo "This will restore the original state: zsd/${REPO}@${BACKUP_SNAPSHOT}"
read -p "Roll back? (y/n): " ROLLBACK

if [ "$ROLLBACK" == "y" ] || [ "$ROLLBACK" == "Y" ]; then
    echo "Rolling back to backup snapshot..."
    sudo zfs rollback -r "zsd/${REPO}@${BACKUP_SNAPSHOT}"
    if [ $? -eq 0 ]; then
        echo "Rollback successful!"
    else
        echo "Rollback failed!"
    fi
else
    echo "Skipping rollback."
fi

echo ""
echo "Test complete."