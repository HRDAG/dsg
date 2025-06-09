# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.06.07
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/utils/execution.py

"""Centralized command execution utilities for DSG.

Consolidates all subprocess patterns across the codebase into a single,
consistent interface with standardized error handling and logging.
"""

import subprocess
from dataclasses import dataclass
from typing import Optional

from loguru import logger


@dataclass
class CommandResult:
    """Result of a command execution."""
    returncode: int
    stdout: str
    stderr: str
    
    @property
    def success(self) -> bool:
        """True if command succeeded (returncode == 0)."""
        return self.returncode == 0


class CommandExecutor:
    """Centralized command execution with consistent error handling.
    
    Replaces 19+ scattered subprocess patterns across backends.py and 
    repository_discovery.py with a single, testable interface.
    """

    @staticmethod
    def run_local(cmd: list[str], timeout: Optional[int] = None, check: bool = True) -> CommandResult:
        """Execute command locally.
        
        Args:
            cmd: Command and arguments as list
            timeout: Optional timeout in seconds
            check: If True, raise ValueError on non-zero exit codes
            
        Returns:
            CommandResult with returncode, stdout, stderr
            
        Raises:
            ValueError: If check=True and command fails
            subprocess.TimeoutExpired: If timeout exceeded
        """
        try:
            logger.debug(f"Executing local command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=timeout
            )
            
            cmd_result = CommandResult(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr
            )
            
            if check and not cmd_result.success:
                error_msg = cmd_result.stderr.strip() if cmd_result.stderr.strip() else f"Command failed with exit code {cmd_result.returncode}"
                raise ValueError(f"Local command failed: {error_msg}")
            
            return cmd_result
            
        except subprocess.TimeoutExpired as e:
            logger.error(f"Command timed out after {timeout}s: {' '.join(cmd)}")
            raise
        except Exception as e:
            logger.error(f"Command execution failed: {' '.join(cmd)} - {e}")
            raise

    @staticmethod 
    def run_ssh(host: str, cmd: list[str], timeout: Optional[int] = None, check: bool = True) -> CommandResult:
        """Execute command via SSH.
        
        Args:
            host: SSH hostname
            cmd: Command and arguments as list
            timeout: Optional timeout in seconds
            check: If True, raise ValueError on non-zero exit codes
            
        Returns:
            CommandResult with returncode, stdout, stderr
            
        Raises:
            ValueError: If check=True and command fails
            subprocess.TimeoutExpired: If timeout exceeded
        """
        ssh_cmd = ["ssh", host] + cmd
        
        try:
            logger.debug(f"Executing SSH command on {host}: {' '.join(cmd)}")
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            cmd_result = CommandResult(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr
            )
            
            if check and not cmd_result.success:
                error_msg = cmd_result.stderr.strip() if cmd_result.stderr.strip() else f"SSH command failed with exit code {cmd_result.returncode}"
                raise ValueError(f"SSH command failed on {host}: {error_msg}")
            
            return cmd_result
            
        except subprocess.TimeoutExpired as e:
            logger.error(f"SSH command timed out after {timeout}s on {host}: {' '.join(cmd)}")
            raise
        except Exception as e:
            logger.error(f"SSH command execution failed on {host}: {' '.join(cmd)} - {e}")
            raise

    @staticmethod
    def run_sudo(cmd: list[str], check: bool = True) -> CommandResult:
        """Execute command with sudo.
        
        Args:
            cmd: Command and arguments as list (sudo will be prepended)
            check: If True, raise ValueError on non-zero exit codes
            
        Returns:
            CommandResult with returncode, stdout, stderr
            
        Raises:
            ValueError: If check=True and command fails
        """
        sudo_cmd = ["sudo"] + cmd
        
        try:
            logger.debug(f"Executing sudo command: {' '.join(cmd)}")
            result = subprocess.run(
                sudo_cmd,
                capture_output=True,
                text=True
            )
            
            cmd_result = CommandResult(
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr
            )
            
            if check and not cmd_result.success:
                error_msg = cmd_result.stderr.strip() if cmd_result.stderr.strip() else f"Sudo command failed with exit code {cmd_result.returncode}"
                raise ValueError(f"Sudo command failed: {error_msg}")
            
            return cmd_result
            
        except Exception as e:
            logger.error(f"Sudo command execution failed: {' '.join(cmd)} - {e}")
            raise

    @staticmethod
    def run_ssh_with_sudo(host: str, cmd: list[str], check: bool = True) -> CommandResult:
        """Execute command with sudo via SSH.
        
        Args:
            host: SSH hostname  
            cmd: Command and arguments as list (sudo will be prepended)
            check: If True, raise ValueError on non-zero exit codes
            
        Returns:
            CommandResult with returncode, stdout, stderr
            
        Raises:
            ValueError: If check=True and command fails
        """
        sudo_cmd = ["sudo"] + cmd
        return CommandExecutor.run_ssh(host, sudo_cmd, check=check)

    @staticmethod
    def run_with_progress(cmd: list[str], verbose: bool = False, check: bool = True) -> CommandResult:
        """Execute command with optional progress output.
        
        Used primarily for rsync operations that may need to show progress.
        
        Args:
            cmd: Command and arguments as list
            verbose: If True, show command output in real-time
            check: If True, raise ValueError on non-zero exit codes
            
        Returns:
            CommandResult with returncode, stdout, stderr
            
        Raises:
            ValueError: If check=True and command fails
        """
        try:
            logger.debug(f"Executing command with progress (verbose={verbose}): {' '.join(cmd)}")
            
            if verbose:
                # Show output in real-time
                result = subprocess.run(cmd, check=False, text=True)
                cmd_result = CommandResult(
                    returncode=result.returncode,
                    stdout="",  # Output was shown in real-time
                    stderr=""
                )
            else:
                # Capture output
                result = subprocess.run(cmd, capture_output=True, text=True)
                cmd_result = CommandResult(
                    returncode=result.returncode,
                    stdout=result.stdout,
                    stderr=result.stderr
                )
            
            if check and not cmd_result.success:
                error_msg = cmd_result.stderr.strip() if cmd_result.stderr.strip() else f"Command failed with exit code {cmd_result.returncode}"
                raise ValueError(f"Command failed: {error_msg}")
            
            return cmd_result
            
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr if hasattr(e, 'stderr') and e.stderr else f"Command failed with exit code {e.returncode}"
            raise ValueError(f"Command failed: {error_msg}")
        except Exception as e:
            logger.error(f"Command execution failed: {' '.join(cmd)} - {e}")
            raise