# Juniper router SNMP → NetBox collector mapping
#
# Polls one or more Juniper routers via SNMP and syncs device records,
# interfaces and IP addresses into NetBox.
#
# Vendor-specific logic (Juniper OID detection, model/version extraction,
# interface type mapping) is handled entirely in this HCL file so that the
# SNMP source adapter remains vendor-agnostic.
#
# Required environment variables:
#   SNMP_HOSTS       Comma-separated list of router hostnames / IP addresses
#   SNMP_COMMUNITY   SNMP v2c community string (default: public)
#   NETBOX_URL       NetBox base URL (e.g. https://netbox.example.com)
#   NETBOX_TOKEN     NetBox API token
#
# Optional:
#   SNMP_VERSION              SNMP version: 1 | 2c | 3  (default: 2c)
#   SNMP_PORT                 UDP port  (default: 161)
#   SNMP_TIMEOUT              Request timeout in seconds  (default: 5)
#   SNMP_RETRIES              Retry count  (default: 1)
#   NETBOX_CACHE_BACKEND      Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL          Redis URL or SQLite path
#   DRY_RUN                   Set to "true" to log payloads without writing
#   COLLECTOR_SYNC_INTERFACES true | false  (default: true)
#
# SNMPv3 additional variables (only when SNMP_VERSION=3):
#   SNMP_V3_USER       SNMPv3 username
#   SNMP_V3_AUTH_PASS  Authentication password
#   SNMP_V3_AUTH_PROTO Auth protocol: md5 | sha | sha256 | sha384 | sha512  (default: sha)
#   SNMP_V3_PRIV_PASS  Privacy (encryption) password
#   SNMP_V3_PRIV_PROTO Privacy protocol: des | aes | aes128 | aes192 | aes256  (default: aes)

source "juniper" {
  api_type   = "snmp"
  url        = "env('SNMP_HOSTS')"
  username   = "env('SNMP_COMMUNITY', 'public')"
  verify_ssl = false

  # These keys are passed through to source.extra
  version  = "env('SNMP_VERSION', '2c')"
  port     = "env('SNMP_PORT', '161')"
  timeout  = "env('SNMP_TIMEOUT', '5')"
  retries  = "env('SNMP_RETRIES', '1')"

  # Juniper-specific enterprise OIDs to fetch per device.
  # The values are added to the device dict under the given field names.
  extra_oids = {
    jnx_model  = "1.3.6.1.4.1.2636.3.1.2.0"
    jnx_serial = "1.3.6.1.4.1.2636.3.1.3.0"
  }
}

netbox {
  url        = "env('NETBOX_URL')"
  token      = "env('NETBOX_TOKEN')"
  cache      = "env('NETBOX_CACHE_BACKEND', 'none')"
  cache_url  = "env('NETBOX_CACHE_URL', '')"
  rate_limit = 0
}

collector {
  max_workers      = 4
  dry_run          = "env('DRY_RUN', 'false')"
  sync_tag         = "snmp-sync"
  regex_dir        = "./regex"
  sync_interfaces  = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
}

# ---------------------------------------------------------------------------
# Network Devices (Juniper routers / switches)
#
# Vendor detection and field extraction are done here in HCL using:
#   sys_object_id — raw sysObjectID OID string (contains Juniper prefix when
#                   the device is a Juniper product)
#   jnx_model     — model string from JUNIPER-MIB jnxBoxDescr (extra_oid)
#   jnx_serial    — serial number from JUNIPER-MIB jnxBoxSerialNo (extra_oid)
#   description   — raw sysDescr string (parsed for model/version fallback)
# ---------------------------------------------------------------------------

object "device" {
  source_collection = "devices"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]
  max_workers       = 4

  # --- prerequisites -------------------------------------------------------

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = {
      # Detect Juniper by checking the Juniper enterprise OID prefix in
      # sysObjectID.  Extend or replace this expression for other vendors.
      name = "when('1.3.6.1.4.1.2636' in source('sys_object_id'), 'Juniper Networks', source('manufacturer') or 'Unknown')"
    }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      # Prefer the dedicated Juniper MIB OID; fall back to sysDescr parsing.
      model        = "source('jnx_model') or regex_extract(source('description'), '(?i)Juniper Networks.+?Inc\\\\. (\\\\S+)') or 'Unknown'"
      manufacturer = "prereq('manufacturer')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Router'" }
    optional = false
  }

  prerequisite "site" {
    method   = "ensure_site"
    args     = { name = "coalesce(source('location'), 'Unknown')" }
    optional = false
  }

  prerequisite "platform" {
    method   = "ensure_platform"
    args     = {
      # Extract Junos version from sysDescr and build a platform name.
      # join() skips None (no match) so the result is 'Junos' when the
      # pattern is not found and 'Junos <version>' when it is.
      name         = "join(' ', ['Junos', regex_extract(source('description'), '(?i)kernel JUNOS (\\\\S+)')])"
      manufacturer = "prereq('manufacturer')"
    }
    optional = true
  }

  # --- top-level fields ----------------------------------------------------

  field "name" {
    value = "coalesce(source('name'), source('host'))"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "site" {
    value = "prereq('site')"
  }

  field "platform" {
    value = "prereq('platform')"
  }

  field "serial" {
    # Prefer the dedicated Juniper MIB serial OID; fall back to the generic
    # serial field (which the SNMP source populates when extra_oids are not
    # configured).
    value = "source('jnx_serial') or source('serial')"
  }

  field "comments" {
    value = "source('description')"
  }

  field "status" {
    value = "'active'"
  }

  field "tags" {
    type  = "tags"
    value = "['snmp-sync']"
  }

  # -------------------------------------------------------------------------
  # Interfaces
  #
  # The source adapter exposes:
  #   name         — ifName (falls back to ifDescr)
  #   label        — ifAlias
  #   if_type      — raw SNMP ifType integer
  #   type         — standard ifType-mapped slug (may be "other" for Ethernet)
  #   mac_address  — formatted MAC address
  #   admin_status — "up" | "down" | "testing"
  #
  # The interface type field uses regex_file() to apply Juniper-specific
  # name-prefix mappings (see regex/juniper-interface-types.csv), falling
  # back to the standard ifType-derived slug for unrecognised names.
  # -------------------------------------------------------------------------

  interface {
    source_items = "interfaces"
    enabled_if   = "collector.sync_interfaces"

    field "name" {
      value = "source('name')"
    }

    field "label" {
      value = "source('label')"
    }

    field "type" {
      # Apply Juniper name-prefix → type mapping via regex_file.
      # The CSV returns an empty string for non-Juniper names so that
      # `or` falls back to the standard ifType-mapped slug.
      value = "regex_file(lower(source('name')), 'juniper-interface-types.csv') or source('type')"
    }

    field "mac_address" {
      value = "source('mac_address')"
    }

    field "mtu" {
      value = "source('mtu')"
    }

    field "enabled" {
      value = "source('admin_status') == 'up'"
    }

    field "tags" {
      type  = "tags"
      value = "['snmp-sync']"
    }

    ip_address {
      source_items = "ip_addresses"
      primary_if   = "first"

      field "address" {
        value = "source('address')"
      }

      field "status" {
        value = "source('status')"
      }

      field "tags" {
        type  = "tags"
        value = "['snmp-sync']"
      }
    }
  }
}
