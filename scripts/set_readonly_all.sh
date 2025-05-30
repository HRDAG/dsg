#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/set_readonly_all.sh

# Set all ZFS repositories and snapshots to read-only
# This is the final cleanup step after migration is complete

# Get the directory containing this script and change to project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Setting all ZFS repository files to read-only"
echo "=============================================="
echo ""
echo "NOTE: This script requires sudo privileges for:"
echo "  - chmod operations on repository files"
echo "  - ZFS commands to verify snapshots"

# Check if running with --dry-run
DRY_RUN=""
if [ "$1" == "--dry-run" ]; then
    DRY_RUN="--dry-run"
    echo -e "${YELLOW}Running in DRY RUN mode${NC}"
fi

echo ""
echo "This will:"
echo "  1. Set all files in /var/repos/zsd/* to read-only (chmod 444)"
echo "  2. Set all directories to readable/executable (chmod 755)"
echo "  3. Verify ZFS snapshots exist (snapshots are inherently read-only)"
echo ""

if [ -z "$DRY_RUN" ]; then
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted"
        exit 0
    fi
fi

# Run the Python script
if PYTHONPATH=. uv run python scripts/migration/set_readonly.py $DRY_RUN --verbose; then
    echo ""
    echo -e "${GREEN}✓ Successfully set all repository files to read-only${NC}"
    
    if [ -z "$DRY_RUN" ]; then
        echo ""
        echo "Migration cleanup complete!"
        echo "All files are now read-only and ZFS snapshots verified."
    fi
else
    echo ""
    echo -e "${RED}✗ Failed to set repositories to read-only${NC}"
    echo "Check logs for details"
    exit 1
fi