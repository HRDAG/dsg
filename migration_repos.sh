#!/bin/bash
# Migration repository list - the 18 repositories to migrate
# Source this file with: source migration_repos.sh

MIGRATION_REPOS="HN HN-indexing IQ CO DA-staten-foil GT GT-AHPN GT-fingerprints GT-legacy GT-media GT-private LK LK-AI LR MX PE PH-killings policeshootingsdata PR-Km0 SDS-492 SFO-pubdef SFO-pubdef-documents SV SY SY-full-conflict SY-OHCHR UK-FA US-cpdmay29jun1 US-CRC US-II-GBV US-IPNO-exonerations US-policeshootingsdatabase"

# Convert to array for easier counting
MIGRATION_REPOS_ARRAY=($MIGRATION_REPOS)

echo "Loaded ${#MIGRATION_REPOS_ARRAY[@]} repositories for migration"