"""Shared pytest fixtures for the clemson-netbox-discovery test suite."""

from __future__ import annotations

import types
from unittest.mock import MagicMock

import pytest

from collector.config import (
    CollectionConfig,
    CollectorOptions,
    SourceConfig,
)
from collector.context import RunContext


# ---------------------------------------------------------------------------
# Source config factories
# ---------------------------------------------------------------------------


@pytest.fixture()
def vmware_config():
    return SourceConfig(
        api_type="vmware",
        url="vcenter.example.com",
        username="admin",
        password="secret",
        verify_ssl=False,
        extra={"fetch_tags": "false"},
    )


@pytest.fixture()
def azure_config():
    return SourceConfig(
        api_type="azure",
        url="",
        username="client-id",
        password="client-secret",
        verify_ssl=True,
        extra={"auth_method": "service_principal", "tenant_id": "tenant-123"},
    )


@pytest.fixture()
def catc_config():
    return SourceConfig(
        api_type="catc",
        url="https://catc.example.com",
        username="admin",
        password="secret",
        verify_ssl=False,
    )


@pytest.fixture()
def ldap_config():
    return SourceConfig(
        api_type="ldap",
        url="ldaps://ldap.example.com",
        username="cn=admin,dc=example,dc=com",
        password="secret",
        verify_ssl=True,
        extra={
            "search_base": "ou=dhcp,dc=example,dc=com",
            "search_filter": "(DirXMLjnsuDHCPAddress=*)",
            "skip_aps": "true",
        },
    )


@pytest.fixture()
def nexus_config():
    return SourceConfig(
        api_type="nexus",
        url="https://ndfc.example.com",
        username="admin",
        password="secret",
        verify_ssl=False,
        extra={"fetch_interfaces": "false"},
    )


@pytest.fixture()
def rest_config():
    return SourceConfig(
        api_type="rest",
        url="https://api.example.com",
        username="user",
        password="pass",
        verify_ssl=True,
        extra={"auth": "basic"},
        collections={
            "nodes": CollectionConfig(
                name="nodes",
                endpoint="/nodes",
                list_key="nodeList",
                detail_endpoint="/nodes/{uuid}",
                detail_id_field="uuid",
            ),
            "switches": CollectionConfig(
                name="switches",
                endpoint="/switches",
                list_key="switchList",
            ),
        },
    )


# ---------------------------------------------------------------------------
# Resolver context fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def resolver_context():
    """Return a minimal RunContext suitable for Resolver tests."""
    opts = CollectorOptions(
        max_workers=4,
        dry_run=False,
        sync_tag="test-sync",
        regex_dir="/tmp/regex",
    )
    ctx = RunContext(
        nb=None,
        source_adapter=None,
        collector_opts=opts,
        regex_dir="/tmp/regex",
        prereqs={},
        source_obj={"name": "test-vm", "memory": 4096, "vcpus": 2},
        parent_nb_obj=None,
        dry_run=False,
    )
    return ctx
