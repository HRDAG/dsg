#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/run_phase3_all.sh

# Run Phase 3 migration (tag migration) on all repositories in /var/repos/zsd

# Note: Don't use set -e since we want to continue processing other repos if one fails

# Get the directory containing this script and change to project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "Phase 3 Migration: Tag Migration for all ZFS repositories"
echo "=========================================================="

# Check if running with --dry-run
DRY_RUN=""
if [ "$1" == "--dry-run" ]; then
    DRY_RUN="--dry-run"
    echo -e "${YELLOW}Running in DRY RUN mode${NC}"
fi

# Count repositories
TOTAL_REPOS=$(ls -1 /var/repos/zsd 2>/dev/null | wc -l)

if [ $TOTAL_REPOS -eq 0 ]; then
    echo -e "${RED}No repositories found in /var/repos/zsd${NC}"
    exit 1
fi

echo "Found $TOTAL_REPOS repositories to process"

# Debug: List all repositories
echo "Repositories found:"
ls -1 /var/repos/zsd | head -5
echo "..."
echo ""

# Track success/failure
SUCCESS_COUNT=0
FAILED_REPOS=""

# Process each repository
for repo_path in /var/repos/zsd/*; do
    if [ -d "$repo_path" ]; then
        repo=$(basename "$repo_path")
        
        echo -e "${YELLOW}Processing repository: $repo${NC}"
        
        # Check if .dsg directory exists
        if [ ! -d "$repo_path/.dsg" ]; then
            echo -e "${RED}  Skipping $repo - no .dsg directory found${NC}"
            echo "  (Phase 2 migration may not have been completed)"
            echo ""
            continue
        fi
        
        # Check if sync-messages.json exists
        if [ ! -f "$repo_path/.dsg/sync-messages.json" ]; then
            echo -e "${RED}  Skipping $repo - no sync-messages.json found${NC}"
            echo "  (Phase 2 migration may not have been completed)"
            echo ""
            continue
        fi
        
        # Check if tag-messages.json already exists
        if [ -f "$repo_path/.dsg/tag-messages.json" ] && [ -z "$DRY_RUN" ]; then
            echo -e "${YELLOW}  Warning: tag-messages.json already exists${NC}"
            read -p "  Overwrite? (y/N) " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                echo "  Skipping $repo"
                echo ""
                continue
            fi
        fi
        
        # Run the migration
        echo "  Running: PYTHONPATH=. uv run python scripts/migration/phase3_migration.py $repo $DRY_RUN"
        if PYTHONPATH=. uv run python scripts/migration/phase3_migration.py "$repo" $DRY_RUN; then
            echo -e "${GREEN}  ✓ Successfully migrated tags for $repo${NC}"
            ((SUCCESS_COUNT++))
        else
            echo -e "${RED}  ✗ Failed to migrate tags for $repo${NC}"
            FAILED_REPOS="$FAILED_REPOS $repo"
        fi
        
        echo ""
    else
        echo "Skipping $(basename "$repo_path") - not a directory"
    fi
done

echo "Finished processing all repositories"

# Summary
echo "=========================================================="
echo "Phase 3 Migration Complete"
echo ""
echo -e "${GREEN}Successful: $SUCCESS_COUNT repositories${NC}"

if [ -n "$FAILED_REPOS" ]; then
    FAILED_COUNT=$(echo $FAILED_REPOS | wc -w)
    echo -e "${RED}Failed: $FAILED_COUNT repositories${NC}"
    echo -e "${RED}Failed repositories:$FAILED_REPOS${NC}"
    
    echo ""
    echo "Check logs in ~/tmp/log/phase3-*.log for details"
    exit 1
else
    echo -e "${GREEN}All repositories processed successfully!${NC}"
    
    if [ -z "$DRY_RUN" ]; then
        echo ""
        echo "Tag migration is complete. You can verify the results with:"
        echo "  cat /var/repos/zsd/REPO/.dsg/tag-messages.json"
    fi
fi