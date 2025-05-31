#!/bin/bash

# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/clone-for-test.sh

set -euo pipefail

# Script to create perfect test copies of DSG repositories with all ZFS snapshots
# Usage: ./clone-for-test.sh SV [host]
# Creates: /var/repos/zsd/test-SV with all snapshots from /var/repos/zsd/SV

# Configuration
DEFAULT_HOST="scott"
DEFAULT_BASE_PATH="/var/repos/zsd"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    echo "Usage: $0 REPO_NAME [HOST]"
    echo ""
    echo "Creates a perfect test copy of a DSG repository with all ZFS snapshots."
    echo ""
    echo "Arguments:"
    echo "  REPO_NAME    Name of existing repository to clone (e.g., 'SV')"
    echo "  HOST         Remote host (default: '$DEFAULT_HOST')"
    echo ""
    echo "Examples:"
    echo "  $0 SV                    # Clone SV repo on default host"
    echo "  $0 HN scott              # Clone HN repo on scott"
    echo "  $0 CO localhost          # Clone CO repo locally"
    echo ""
    echo "Output:"
    echo "  Creates: \$HOST:$DEFAULT_BASE_PATH/test-REPO_NAME"
    echo "  Includes: All ZFS snapshots and metadata from original"
    exit 1
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    exit 1
}

run_cmd() {
    local host="$1"
    local cmd="$2"
    
    # Check if we're already on the target host
    local current_hostname
    current_hostname=$(hostname -s)
    
    if [[ "$host" == "localhost" ]] || [[ "$host" == "$current_hostname" ]]; then
        eval "$cmd"
    else
        ssh "$host" "$cmd"
    fi
}

check_repo_exists() {
    local host="$1"
    local repo_path="$2"
    
    log_info "Checking if source repository exists: $host:$repo_path"
    
    if ! run_cmd "$host" "test -d '$repo_path'"; then
        log_error "Source repository not found: $host:$repo_path"
    fi
    
    if ! run_cmd "$host" "test -d '$repo_path/.dsg'"; then
        log_error "Source is not a DSG repository (no .dsg directory): $host:$repo_path"
    fi
    
    log_success "Source repository found and valid"
}

check_dest_not_exists() {
    local host="$1"
    local dest_path="$2"
    
    log_info "Checking if destination doesn't already exist: $host:$dest_path"
    
    if run_cmd "$host" "test -d '$dest_path'"; then
        log_error "Destination already exists: $host:$dest_path (remove it first if you want to recreate)"
    fi
    
    log_success "Destination path is available"
}

get_zfs_dataset() {
    local host="$1"
    local repo_path="$2"
    
    log_info "Finding ZFS dataset for: $host:$repo_path"
    
    # Get the dataset that contains this mountpoint
    local dataset
    dataset=$(run_cmd "$host" "zfs list -H -o name,mountpoint | awk '\$2 == \"$repo_path\" {print \$1}'" 2>/dev/null)
    
    if [[ -z "$dataset" ]]; then
        log_error "No ZFS dataset found with mountpoint: $repo_path"
    fi
    
    log_success "Found ZFS dataset: $dataset"
    echo "$dataset"
}

clone_zfs_repo() {
    local host="$1"
    local source_dataset="$2"
    local dest_dataset="$3"
    local dest_path="$4"
    
    log_info "Cloning ZFS repository with all snapshots..."
    log_info "  Source: $source_dataset"
    log_info "  Destination: $dest_dataset"
    
    # Use ZFS send/receive to copy all snapshots
    # -R flag includes all snapshots recursively
    local latest_snapshot
    latest_snapshot=$(run_cmd "$host" "zfs list -H -t snapshot -o name '$source_dataset' | tail -1")
    
    if [[ -z "$latest_snapshot" ]]; then
        log_error "No snapshots found for dataset: $source_dataset"
    fi
    
    log_info "Latest snapshot: $latest_snapshot"
    log_info "Sending all snapshots to new dataset..."
    
    # ZFS send/receive with all snapshots
    run_cmd "$host" "sudo zfs send -R '$latest_snapshot' | sudo zfs receive '$dest_dataset'"
    
    # Set mountpoint for the new dataset
    run_cmd "$host" "sudo zfs set mountpoint='$dest_path' '$dest_dataset'"
    
    log_success "ZFS clone completed"
}

verify_clone() {
    local host="$1"
    local dest_path="$2"
    local dest_dataset="$3"
    
    log_info "Verifying cloned repository..."
    
    # Check directory exists and is mounted
    if ! run_cmd "$host" "test -d '$dest_path'"; then
        log_error "Destination directory not found after clone: $dest_path"
    fi
    
    # Check .dsg directory exists
    if ! run_cmd "$host" "test -d '$dest_path/.dsg'"; then
        log_error "Cloned repository missing .dsg directory: $dest_path"
    fi
    
    # Check we have snapshots
    local snapshot_count
    snapshot_count=$(run_cmd "$host" "zfs list -H -t snapshot '$dest_dataset' | wc -l")
    
    if [[ "$snapshot_count" -eq 0 ]]; then
        log_error "No snapshots found in cloned dataset: $dest_dataset"
    fi
    
    log_success "Clone verification passed"
    log_info "  Snapshots copied: $snapshot_count"
    log_info "  Repository path: $host:$dest_path"
    log_info "  Dataset: $dest_dataset"
}

main() {
    local repo_name="${1:-}"
    local host="${2:-$DEFAULT_HOST}"
    
    if [[ -z "$repo_name" ]]; then
        usage
    fi
    
    # Construct paths
    local source_path="$DEFAULT_BASE_PATH/$repo_name"
    local dest_name="test-$repo_name"
    local dest_path="$DEFAULT_BASE_PATH/$dest_name"
    
    log_info "Starting repository clone operation"
    log_info "  Source: $host:$source_path"
    log_info "  Destination: $host:$dest_path"
    
    # Validation steps
    check_repo_exists "$host" "$source_path"
    check_dest_not_exists "$host" "$dest_path"
    
    # Get ZFS dataset names
    local source_dataset
    source_dataset=$(get_zfs_dataset "$host" "$source_path")
    
    # Construct destination dataset name
    local source_base
    source_base=$(dirname "$source_dataset")
    local dest_dataset="$source_base/$dest_name"
    
    # Perform the clone
    clone_zfs_repo "$host" "$source_dataset" "$dest_dataset" "$dest_path"
    
    # Verify the result
    verify_clone "$host" "$dest_path" "$dest_dataset"
    
    log_success "Repository clone completed successfully!"
    echo ""
    echo "Test repository ready at: $host:$dest_path"
    echo "You can now safely test DSG operations on this copy."
    echo ""
    echo "To use with dsg commands, you may need to update .dsgconfig.yml"
    echo "in your project to point to the test repository."
}

# Handle Ctrl+C gracefully
trap 'log_error "Operation interrupted by user"' INT

# Run main function
main "$@"