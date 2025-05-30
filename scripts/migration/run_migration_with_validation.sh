#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/run_migration_with_validation.sh

# Run Phase 2 migration with validation for a given repository
# Usage: ./run_migration_with_validation.sh REPO [LIMIT]
#
# Examples:
#   ./run_migration_with_validation.sh SV        # Migrate all snapshots
#   ./run_migration_with_validation.sh SV 5      # Migrate first 5 snapshots

set -e

REPO="${1:?Usage: $0 REPO [LIMIT]}"
LIMIT="${2:-0}"  # Default to 0 (all snapshots)

echo "=== Phase 2 Migration with Validation ==="
echo "Repository: $REPO"
echo "Limit: ${LIMIT:-all}"
echo "Source: /var/repos/btrsnap/${REPO}-norm"
echo "Target: /var/repos/zsd/${REPO}"
echo ""

# Check if source exists
if [ ! -d "/var/repos/btrsnap/${REPO}-norm" ]; then
    echo "ERROR: Source repository not found: /var/repos/btrsnap/${REPO}-norm"
    echo "Has Phase 1 normalization been completed?"
    exit 1
fi

# Check if target already exists
if zfs list "zsd/${REPO}" >/dev/null 2>&1; then
    echo "WARNING: Target dataset already exists: zsd/${REPO}"
    echo "Note: migrate.py will destroy and recreate it automatically"
    echo ""
fi

# Confirm
echo ""
read -p "Proceed with migration? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Migration cancelled"
    exit 0
fi


# Step 1: Run migration
echo ""
echo "==> Step 1: Running migration..."
echo "Log file: /home/pball/tmp/log/migration-${REPO}-$(date +%Y%m%d-%H%M%S).log"

# Change to project root directory for proper imports
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

if [ "$LIMIT" -eq 0 ]; then
    PYTHONPATH=. uv run python scripts/migration/migrate.py "$REPO"
else
    PYTHONPATH=. uv run python scripts/migration/migrate.py "$REPO" --limit="$LIMIT"
fi

if [ $? -ne 0 ]; then
    echo "ERROR: Migration failed!"
    exit 1
fi

# Step 2: Run validation
echo ""
echo "==> Step 2: Validating migration..."

# Use different sampling based on whether we did a full or partial migration
if [ "$LIMIT" -eq 0 ]; then
    # Full migration - sample more files
    PYTHONPATH=. uv run python scripts/migration/validate_migration.py "$REPO" --sample-files=100
else
    # Partial migration - check all files
    PYTHONPATH=. uv run python scripts/migration/validate_migration.py "$REPO" --sample-files=0
fi

if [ $? -ne 0 ]; then
    echo "ERROR: Validation failed!"
    exit 1
fi

echo ""
echo "=== Migration completed successfully! ==="