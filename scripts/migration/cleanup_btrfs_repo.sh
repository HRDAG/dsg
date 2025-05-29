#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/migration/cleanup_btrfs_repo.sh

set -eo pipefail

# Safety function to clean up btrfs repository with subvolumes
# ALWAYS defaults to dry-run for safety
cleanup_btrfs_repo() {
    local repo_path="$1"
    local dry_run="${2:-true}" # Default to dry-run for safety

    # Validate input path - must be under /var/repos/btrsnap and end with -norm
    if [[ ! "$repo_path" =~ ^/var/repos/btrsnap/.*-norm$ ]]; then
        echo "ERROR: Path must be under /var/repos/btrsnap/ and end with -norm"
        echo "ERROR: Got: $repo_path"
        echo "ERROR: This safety check prevents accidental deletion of source repositories"
        return 1
    fi

    if [ ! -d "$repo_path" ]; then
        echo "Directory $repo_path doesn't exist, nothing to clean"
        return 0
    fi

    echo "========================================"
    echo "BTRFS REPOSITORY CLEANUP"
    echo "Target: $repo_path"
    echo "Dry run: $dry_run"
    echo "========================================"

    # Find all subvolumes in the directory
    echo "Scanning for subvolumes..."
    local repo_name=$(basename "$repo_path")
    local subvols=$(sudo btrfs subvolume list /var/repos/btrsnap 2>/dev/null |
        awk '{print $NF}' |
        grep "^$repo_name/" |
        sort -r) # Reverse order for proper deletion (deepest first)

    # Show what we found
    if [ -n "$subvols" ]; then
        echo ""
        echo "Found $(echo "$subvols" | wc -l) subvolumes to delete:"
        echo "$subvols" | while read -r subvol; do
            echo "  - /var/repos/btrsnap/$subvol"
        done
    else
        echo "No subvolumes found in $repo_path"
    fi

    # Show regular files/directories
    echo ""
    echo "Regular directories and files:"
    ls -la "$repo_path" | head -10
    if [ $(ls -la "$repo_path" | wc -l) -gt 11 ]; then
        echo "  ... ($(ls -1 "$repo_path" | wc -l) items total)"
    fi

    echo ""
    # echo "Disk usage:"
    # du -sh "$repo_path"

    if [ "$dry_run" = "true" ]; then
        echo ""
        echo "=========================="
        echo "DRY RUN - NO CHANGES MADE"
        echo "=========================="
        echo "To actually delete, run:"
        echo "  cleanup_btrfs_repo '$repo_path' false"
        return 0
    fi

    # Confirm before proceeding
    echo ""
    echo "=========================="
    echo "READY TO DELETE"
    echo "=========================="
    echo "This will permanently delete:"
    echo "- $(echo "$subvols" | wc -l) btrfs subvolumes"
    echo "- All files and directories in $repo_path"
    echo ""
    read -p "Are you absolutely sure? Type 'DELETE' to confirm: " confirm

    if [ "$confirm" != "DELETE" ]; then
        echo "Aborted - no changes made"
        return 1
    fi

    # Delete subvolumes first
    if [ -n "$subvols" ]; then
        echo ""
        echo "Deleting subvolumes..."
        echo "$subvols" | while read -r subvol; do
            if [ -n "$subvol" ]; then
                local full_path="/var/repos/btrsnap/$subvol"
                echo "Deleting subvolume: $full_path"
                if sudo btrfs subvolume delete "$full_path"; then
                    echo "  ✓ Successfully deleted $full_path"
                else
                    echo "  ✗ Failed to delete $full_path"
                    return 1
                fi
            fi
        done
    fi

    # Remove remaining regular directories/files
    echo ""
    echo "Removing remaining files and directories..."
    if sudo rm -rf "$repo_path"; then
        echo "  ✓ Successfully removed $repo_path"
    else
        echo "  ✗ Failed to remove $repo_path"
        return 1
    fi

    echo ""
    echo "========================================"
    echo "CLEANUP COMPLETE"
    echo "========================================"
}

# If script is run directly (not sourced), process command line arguments
if [[ "${BASH_SOURCE[0]:-}" == "${0}" ]]; then
    if [ $# -eq 0 ]; then
        echo "Usage: $0 <repo_path> [dry_run]"
        echo "  repo_path: Path to repository (must end with -norm)"
        echo "  dry_run: true (default) or false"
        echo ""
        echo "Examples:"
        echo "  $0 /var/repos/btrsnap/LK-norm          # Dry run"
        echo "  $0 /var/repos/btrsnap/LK-norm true     # Dry run"
        echo "  $0 /var/repos/btrsnap/LK-norm false    # Actually delete"
        exit 1
    fi

    cleanup_btrfs_repo "$1" "${2:-true}"
fi
