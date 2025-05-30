#!/bin/bash
# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.28
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/list_btrsnap_symlinks.sh

# List all symlinks in first level of non-norm btrsnap repositories

echo "Symlinks in /var/repos/btrsnap repositories (excluding -norm repos):"
echo "=================================================================="

for repo in /var/repos/btrsnap/*; do
    # Skip if not a directory
    [ ! -d "$repo" ] && continue
    
    # Skip -norm repositories
    [[ "$repo" == *-norm ]] && continue
    
    repo_name=$(basename "$repo")
    
    # Check if there are any symlinks
    symlink_count=$(find "$repo" -maxdepth 1 -type l 2>/dev/null | wc -l)
    
    if [ $symlink_count -gt 0 ]; then
        echo ""
        echo "Repository: $repo_name"
        echo "----------------------------------------"
        
        # List all symlinks with their targets
        for link in "$repo"/*; do
            if [ -L "$link" ]; then
                link_name=$(basename "$link")
                target=$(readlink "$link")
                printf "%-20s -> %s\n" "$link_name" "$target"
            fi
        done
    fi
done

echo ""
echo "=================================================================="
echo "Summary of unique symlink patterns:"
echo ""

# Collect all symlink names for pattern analysis
all_links=""
for repo in /var/repos/btrsnap/*; do
    [ ! -d "$repo" ] && continue
    [[ "$repo" == *-norm ]] && continue
    
    for link in "$repo"/*; do
        if [ -L "$link" ]; then
            link_name=$(basename "$link")
            all_links="$all_links$link_name\n"
        fi
    done
done

# Show unique symlink names sorted
echo "$all_links" | sort -u | grep -E '^v[0-9]' | head -20
echo ""
echo "Non-version symlinks:"
echo "$all_links" | sort -u | grep -v -E '^v[0-9]' | grep -v '^HEAD$' | head -20