# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# src/dsg/host_utils.py

"""Host detection utilities for determining local vs remote hosts."""

import socket
from typing import Final

# Common localhost identifiers
_LOCALHOST_NAMES: Final[frozenset[str]] = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "127.0.0.1",
        "::1",
        "0.0.0.0",
    }
)


def is_local_host(host: str) -> bool:
    """Determine if a hostname refers to the local machine.

    Args:
        host: Hostname, FQDN, or IP address to check

    Returns:
        True if the host refers to the local machine, False otherwise

    This function handles:
    - Standard localhost identifiers (localhost, 127.0.0.1, ::1)
    - Current machine hostname and FQDN
    - Local network interface addresses (IPv4 and IPv6)
    - Case-insensitive hostname matching

    Examples:
        >>> is_local_host("localhost")
        True
        >>> is_local_host("127.0.0.1")
        True
        >>> is_local_host("remote-server")
        False
    """
    if not host:
        return False

    # Normalize to lowercase for comparison
    host_lower = host.lower().strip()

    # Check common localhost identifiers
    if host_lower in _LOCALHOST_NAMES:
        return True

    # Get local machine identifiers
    try:
        current_hostname = socket.gethostname().lower()
        current_fqdn = socket.getfqdn().lower()

        # Check hostname and FQDN
        if host_lower in {current_hostname, current_fqdn}:
            return True

        # Check if it's a local network interface address
        if _is_local_interface_address(host):
            return True

    except (socket.error, OSError):
        # If we can't get local machine info, be conservative
        pass

    return False


def _is_local_interface_address(host: str) -> bool:
    """Check if host is bound to a local network interface.

    Args:
        host: IP address to check

    Returns:
        True if the IP is bound to a local interface, False otherwise
    """
    # Empty string is not a valid IP address
    if not host.strip():
        return False

    try:
        # Try to bind to the address - if successful, it's local
        # This works for both IPv4 and IPv6
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, 0))
            return True
    except (socket.error, OSError, ValueError):
        # Try IPv6 if IPv4 failed
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
                sock.bind((host, 0))
                return True
        except (socket.error, OSError, ValueError):
            pass

    return False
