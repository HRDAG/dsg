# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.05
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# scripts/Makefile

# DSG Scripts Makefile
# Provides convenient targets for development and validation tasks

.PHONY: help validate-ssh-localhost validate-ssh-remote validate-ssh-full clean

# Default target
help:
	@echo "DSG Scripts - Available targets:"
	@echo ""
	@echo "  validate-ssh-localhost   Test SSH file operations on localhost (optimization test)"
	@echo "  validate-ssh-remote      Test SSH file operations on remote host (interactive)"
	@echo "  validate-ssh-full        Run complete SSH validation suite"
	@echo "  clean                    Clean up any temporary test files"
	@echo ""
	@echo "Usage:"
	@echo "  make validate-ssh-localhost    # Safe localhost-only testing"
	@echo "  make validate-ssh-remote       # Prompts for remote hostname"
	@echo "  make validate-ssh-full         # Complete validation"

# Test SSH operations on localhost (safe, no remote dependencies)
validate-ssh-localhost:
	@echo "=== SSH File Operations Validation: Localhost Only ==="
	@echo "Testing SSH-to-localhost optimization and file operations..."
	export UV_LINK_MODE=copy && uv run python validate_ssh_file_ops.py --localhost-only

# Test SSH operations on remote host (interactive)
validate-ssh-remote:
	@echo "=== SSH File Operations Validation: Remote Testing ==="
	@echo "This will prompt for remote hostname and test real SSH connections..."
	export UV_LINK_MODE=copy && uv run python validate_ssh_file_ops.py --remote-only

# Complete SSH validation suite (localhost + remote)
validate-ssh-full:
	@echo "=== Complete SSH File Operations Validation ==="
	@echo "Testing both localhost optimization and remote SSH operations..."
	export UV_LINK_MODE=copy && uv run python validate_ssh_file_ops.py

# Clean up any temporary test files or directories
clean:
	@echo "Cleaning up SSH validation temporary files..."
	@find /tmp -name "dsg-ssh-*" -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name "*.pyc" -delete 2>/dev/null || true
	@find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup complete."

# Development targets
.PHONY: dev-setup dev-clean

# Set up development environment for SSH testing
dev-setup:
	@echo "Setting up SSH validation development environment..."
	@echo "Ensuring SSH keys are set up for localhost..."
	@ssh localhost echo "SSH to localhost working" || echo "WARNING: SSH to localhost not configured"
	@echo "Development setup complete."

# Clean development artifacts
dev-clean: clean
	@echo "Development cleanup complete."