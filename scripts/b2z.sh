#!/bin/bash
# b2z.sh - Migrate snapshots from btrfs to ZFS with metadata
#
# This script destroys an existing ZFS dataset and its snapshots,
# then uses btr-to-zfs-dsg.py to recreate it with proper metadata.
#
# Usage:
#   ./b2z.sh --repo=XX [--limit=N] [--dryrun] [--force]
#
# Options:
#   --repo=XX    The repository name (required)
#   --limit=N    Limit the number of snapshots to process (optional)
#   --dryrun     Don't actually run commands, just print them
#   --force      Skip safety confirmation prompt
#
# Example:
#   ./b2z.sh --repo=SV --limit=5
#

set -e  # Exit on any error

# Default values
REPO=""
LIMIT=0
DRYRUN=0
FORCE=0
VERBOSE=0
LOG_FILE=""

# Parse arguments
for arg in "$@"; do
  case $arg in
    --repo=*)
      REPO="${arg#*=}"
      shift
      ;;
    --limit=*)
      LIMIT="${arg#*=}"
      shift
      ;;
    --dryrun)
      DRYRUN=1
      shift
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --verbose)
      VERBOSE=1
      shift
      ;;
    *)
      echo "Unknown argument: $arg"
      echo "Usage: $0 --repo=XX [--limit=N] [--dryrun] [--force] [--verbose]"
      exit 1
      ;;
  esac
done

# Validate required arguments
if [ -z "$REPO" ]; then
  echo "Error: --repo argument is required"
  echo "Usage: $0 --repo=XX [--limit=N] [--dryrun] [--force]"
  exit 1
fi

# Setup logging
timestamp=$(date +"%Y%m%d-%H%M%S")
LOG_DIR="/home/pball/tmp/log"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/migration-${REPO}-${timestamp}.log"
echo "Logging to $LOG_FILE"

# Helper function to run or echo commands based on dryrun flag
run_cmd() {
  echo "COMMAND: $*" | tee -a "$LOG_FILE"
  if [ $DRYRUN -eq 0 ]; then
    eval "$@" 2>&1 | tee -a "$LOG_FILE"
    return ${PIPESTATUS[0]}
  else
    echo "[DRYRUN] Would execute: $*" | tee -a "$LOG_FILE"
    return 0
  fi
}

echo "=== Starting migration for repository: $REPO ===" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"

# Check if source directory exists
BTRFS_PATH="/var/repos/btrsnap/${REPO}"
if [ ! -d "$BTRFS_PATH" ]; then
  echo "Error: Source directory $BTRFS_PATH does not exist" | tee -a "$LOG_FILE"
  exit 1
fi

# Ensure that Python packages are installed
echo "Ensuring Python dependencies are installed..." | tee -a "$LOG_FILE"
run_cmd "cd /home/pball/git/dsg && poetry install"

# Check if destination dataset exists
ZFS_DATASET="zsd/${REPO}"
ZFS_PATH="/var/repos/${ZFS_DATASET}"

echo "Checking if ZFS dataset exists: $ZFS_DATASET" | tee -a "$LOG_FILE"
if sudo zfs list "$ZFS_DATASET" > /dev/null 2>&1; then
  echo "Warning: ZFS dataset $ZFS_DATASET already exists" | tee -a "$LOG_FILE"
  
  # If not forced, ask for confirmation
  if [ $FORCE -eq 0 ] && [ $DRYRUN -eq 0 ]; then
    read -p "This will destroy the existing dataset and all its snapshots. Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Operation cancelled by user" | tee -a "$LOG_FILE"
      exit 1
    fi
  fi
  
  # In dry run mode, just display the command
  if [ $DRYRUN -eq 1 ]; then
    echo "COMMAND: sudo zfs destroy -r $ZFS_DATASET" | tee -a "$LOG_FILE"
    echo "[DRYRUN] Would execute: sudo zfs destroy -r $ZFS_DATASET" | tee -a "$LOG_FILE"
  else
    echo "Destroying existing ZFS dataset: $ZFS_DATASET" | tee -a "$LOG_FILE"
    echo "COMMAND: sudo zfs destroy -r $ZFS_DATASET" | tee -a "$LOG_FILE"
    sudo zfs destroy -r "$ZFS_DATASET" 2>&1 | tee -a "$LOG_FILE"
  fi
fi

# Check if required commands are available
for cmd in sudo zfs rsync lz4; do
  if ! command -v $cmd &> /dev/null; then
    echo "Error: Required command '$cmd' not found" | tee -a "$LOG_FILE"
    exit 1
  fi
done

# Create limit option string if limit is specified
LIMIT_OPT=""
if [ $LIMIT -gt 0 ]; then
  LIMIT_OPT="--limit=$LIMIT"
  echo "Limiting to $LIMIT snapshots" | tee -a "$LOG_FILE"
fi

# Step 1: Run migration with basic validation
echo "Step 1: Running migration with basic validation" | tee -a "$LOG_FILE"
run_cmd "cd /home/pball/git/dsg && poetry run python -c 'import sys; print(sys.path)'"

# Build command with optional verbose flag
VERBOSE_OPT=""
if [ $VERBOSE -eq 1 ]; then
  VERBOSE_OPT="--verbose"
fi

if run_cmd "cd /home/pball/git/dsg && poetry run python scripts/btr-to-zfs-dsg.py $REPO --validation=basic $LIMIT_OPT $VERBOSE_OPT"; then
  echo "Basic migration completed successfully" | tee -a "$LOG_FILE"
else
  echo "Error: Basic migration failed" | tee -a "$LOG_FILE"
  exit 1
fi

# Step 2: Run final validation on all migrated snapshots
echo "Step 2: Running full validation on all migrated snapshots" | tee -a "$LOG_FILE"
VALIDATE_LIMIT_OPT=""
if [ $LIMIT -gt 0 ]; then
  VALIDATE_LIMIT_OPT="--limit=$LIMIT"
fi

# Add verbose flag to validation if needed
VALIDATE_VERBOSE_OPT=""
if [ $VERBOSE -eq 1 ]; then
  VALIDATE_VERBOSE_OPT="--verbose"
fi

if run_cmd "cd /home/pball/git/dsg && poetry run python scripts/validate_migration.py --repo=$REPO $VALIDATE_LIMIT_OPT $VALIDATE_VERBOSE_OPT"; then
  echo "Validation completed successfully" | tee -a "$LOG_FILE"
else
  echo "Warning: Validation reported some issues, check the log file for details" | tee -a "$LOG_FILE"
fi

echo "=== Migration process completed for repository: $REPO ===" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"

# Print summary
if [ $LIMIT -gt 0 ]; then
  echo "Note: Only $LIMIT snapshots were processed due to --limit option" | tee -a "$LOG_FILE"
  echo "To process all remaining snapshots, run without the --limit option" | tee -a "$LOG_FILE"
fi

exit 0