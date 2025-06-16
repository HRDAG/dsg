#!/bin/bash
# Test migration script for individual repositories
# Source the repository list
source migration_repos.sh

echo "=== Phase 1 Migration Test Script ==="
echo "Total repositories to migrate: ${#MIGRATION_REPOS_ARRAY[@]}"
echo ""

# Test with a single repository first
test_repo="SV"  # Change this to test different repos

echo "=== Testing single repository: $test_repo ==="

# Check if repo exists
if [ ! -d "/var/repos/btrsnap/$test_repo" ]; then
    echo "ERROR: Repository /var/repos/btrsnap/$test_repo does not exist!"
    exit 1
fi

# Check current status
echo "Checking normalization status..."
cd /home/pball/projects/dsg-migration-recovery
PYTHONPATH=. uv run python scripts/migration/phase1_normalize_cow.py status "$test_repo"

echo ""
echo "=== To run normalization (DRY RUN first): ==="
echo "PYTHONPATH=. uv run python scripts/migration/phase1_normalize_cow.py normalize --dry-run --verbose \"$test_repo\""

echo ""
echo "=== To run actual normalization: ==="
echo "PYTHONPATH=. uv run python scripts/migration/phase1_normalize_cow.py normalize --verbose \"$test_repo\""

echo ""
echo "=== For batch processing all 18 repos: ==="
echo "for repo in \$MIGRATION_REPOS; do"
echo "    echo \"Processing: \$repo\""
echo "    PYTHONPATH=. uv run python scripts/migration/phase1_normalize_cow.py normalize --verbose \"\$repo\""
echo "done"