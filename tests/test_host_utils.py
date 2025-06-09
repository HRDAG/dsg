# Author: PB & Claude
# Maintainer: PB
# Original date: 2025.05.30
# License: (c) HRDAG, 2025, GPL-2 or newer
#
# ------
# tests/test_host_utils.py

"""Tests for host detection utilities."""

import socket
from unittest.mock import patch, Mock

import pytest

from dsg.host_utils import is_local_host, _is_local_interface_address


class TestIsLocalHost:
    """Test the is_local_host function."""

    def test_empty_or_none_host(self):
        """Test empty or None host values."""
        assert not is_local_host("")
        assert not is_local_host("   ")

    def test_localhost_identifiers(self):
        """Test common localhost identifiers."""
        localhost_names = [
            "localhost",
            "LOCALHOST",  # case insensitive
            "localhost.localdomain",
            "127.0.0.1",
            "::1",
            "0.0.0.0",
        ]

        for host in localhost_names:
            assert is_local_host(host), f"Failed for {host}"

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped."""
        assert is_local_host("  localhost  ")
        assert is_local_host("\tlocalhost\n")
        assert is_local_host("  127.0.0.1  ")

    @patch("socket.gethostname")
    @patch("socket.getfqdn")
    def test_current_hostname_and_fqdn(self, mock_getfqdn, mock_gethostname):
        """Test detection of current machine hostname and FQDN."""
        mock_gethostname.return_value = "test-machine"
        mock_getfqdn.return_value = "test-machine.example.com"

        assert is_local_host("test-machine")
        assert is_local_host("TEST-MACHINE")  # case insensitive
        assert is_local_host("test-machine.example.com")
        assert is_local_host("TEST-MACHINE.EXAMPLE.COM")

    @patch("socket.gethostname")
    @patch("socket.getfqdn")
    def test_socket_error_handling(self, mock_getfqdn, mock_gethostname):
        """Test graceful handling of socket errors."""
        mock_gethostname.side_effect = socket.error("Network error")
        mock_getfqdn.side_effect = socket.error("Network error")

        # Should still work for localhost identifiers
        assert is_local_host("localhost")
        assert is_local_host("127.0.0.1")

        # Should return False for unknown hosts when socket calls fail
        assert not is_local_host("unknown-host")

    @patch("dsg.system.host_utils._is_local_interface_address")
    @patch("socket.gethostname")
    @patch("socket.getfqdn")
    def test_local_interface_check(
        self, mock_getfqdn, mock_gethostname, mock_interface_check
    ):
        """Test that local interface addresses are detected."""
        mock_gethostname.return_value = "test-machine"
        mock_getfqdn.return_value = "test-machine.example.com"
        mock_interface_check.return_value = True

        # Should call interface check for non-hostname matches
        assert is_local_host("192.168.1.100")
        mock_interface_check.assert_called_once_with("192.168.1.100")

    def test_remote_hosts(self):
        """Test that clearly remote hosts return False."""
        remote_hosts = [
            "remote-server",
            "example.com",
            "8.8.8.8",  # Google DNS
            "2001:4860:4860::8888",  # Google IPv6 DNS
            "not-this-machine",
        ]

        for host in remote_hosts:
            assert not is_local_host(
                host
            ), f"Incorrectly identified {host} as local"


class TestIsLocalInterfaceAddress:
    """Test the _is_local_interface_address function."""

    def test_localhost_ip_addresses(self):
        """Test that localhost IP addresses are detected as local."""
        assert _is_local_interface_address("127.0.0.1")
        assert _is_local_interface_address("::1")

    def test_invalid_ip_addresses(self):
        """Test that invalid IP addresses return False."""
        invalid_ips = [
            "not.an.ip",
            "999.999.999.999",
            "invalid:ipv6::address",
            "",
            "203.0.113.1",  # TEST-NET-3, should not be bound locally
        ]

        for ip in invalid_ips:
            assert not _is_local_interface_address(
                ip
            ), f"Incorrectly identified {ip} as local interface"

    @patch("socket.socket")
    def test_ipv4_bind_success(self, mock_socket):
        """Test successful IPv4 binding indicates local interface."""
        mock_sock = Mock()
        mock_socket.return_value.__enter__.return_value = mock_sock
        mock_sock.bind.return_value = None  # Successful bind

        assert _is_local_interface_address("192.168.1.100")
        mock_socket.assert_called_with(socket.AF_INET, socket.SOCK_STREAM)
        mock_sock.bind.assert_called_once_with(("192.168.1.100", 0))

    @patch("socket.socket")
    def test_ipv4_bind_failure_tries_ipv6(self, mock_socket):
        """Test that IPv4 bind failure triggers IPv6 attempt."""
        # Create mock socket instances
        ipv4_sock = Mock()
        ipv6_sock = Mock()

        # IPv4 socket fails to bind
        ipv4_sock.bind.side_effect = socket.error("Cannot bind")
        # IPv6 socket succeeds
        ipv6_sock.bind.return_value = None

        # Create context manager mocks
        ipv4_context = Mock()
        ipv4_context.__enter__ = Mock(return_value=ipv4_sock)
        ipv4_context.__exit__ = Mock(return_value=False)

        ipv6_context = Mock()
        ipv6_context.__enter__ = Mock(return_value=ipv6_sock)
        ipv6_context.__exit__ = Mock(return_value=False)

        # Set up socket creation to return context managers
        mock_socket.side_effect = [ipv4_context, ipv6_context]

        assert _is_local_interface_address("2001:db8::1")

        # Should have tried both AF_INET and AF_INET6
        assert mock_socket.call_count == 2
        mock_socket.assert_any_call(socket.AF_INET, socket.SOCK_STREAM)
        mock_socket.assert_any_call(socket.AF_INET6, socket.SOCK_STREAM)

    @patch("socket.socket")
    def test_both_bind_failures(self, mock_socket):
        """Test that both IPv4 and IPv6 bind failures return False."""
        mock_sock = Mock()
        mock_sock.bind.side_effect = socket.error("Cannot bind")
        mock_socket.return_value.__enter__.return_value = mock_sock

        assert not _is_local_interface_address("203.0.113.1")

        # Should have tried both protocols
        assert mock_socket.call_count == 2


class TestIntegration:
    """Integration tests using real network interfaces."""

    def test_real_localhost_detection(self):
        """Test with real localhost addresses."""
        # These should always work on any system
        assert is_local_host("localhost")
        assert is_local_host("127.0.0.1")

    def test_real_hostname_detection(self):
        """Test with real system hostname."""
        try:
            hostname = socket.gethostname()
            if hostname:
                assert is_local_host(hostname)
        except socket.error:
            pytest.skip("Unable to get system hostname")

    def test_real_fqdn_detection(self):
        """Test with real system FQDN."""
        try:
            fqdn = socket.getfqdn()
            # Only test if FQDN is different from hostname and looks valid
            if fqdn and "." in fqdn and fqdn != socket.gethostname():
                assert is_local_host(fqdn)
        except socket.error:
            pytest.skip("Unable to get system FQDN")

    def test_public_dns_servers_not_local(self):
        """Test that well-known public DNS servers are not detected as local."""
        public_dns = [
            "8.8.8.8",  # Google
            "1.1.1.1",  # Cloudflare
            "208.67.222.222",  # OpenDNS
        ]

        for dns in public_dns:
            assert not is_local_host(
                dns
            ), f"Public DNS {dns} incorrectly identified as local"
