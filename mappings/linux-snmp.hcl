# Linux server (net-snmp) SNMP → NetBox collector mapping
#
# Polls Linux servers running the net-snmp daemon (snmpd) via SNMP and syncs
# device records, interfaces, and IP addresses into NetBox.
#
# Vendor-specific logic (NET-SNMP OID detection, kernel version extraction,
# interface type mapping) is handled entirely in this HCL file so that the
# SNMP source adapter remains vendor-agnostic.
#
# NET-SNMP does not expose hardware serial or model via standard MIBs.
# If snmpd is extended with nsExtend or pass_persist directives (e.g. to run
# "dmidecode"), you can add the resulting OIDs to the extra_oids block below.
#
# Required environment variables:
#   SNMP_HOSTS       Comma-separated list of server hostnames / IP addresses
#   SNMP_COMMUNITY   SNMP v2c community string (default: public)
#   NETBOX_URL       NetBox base URL (e.g. https://netbox.example.com)
#   NETBOX_TOKEN     NetBox API token
#
# Optional:
#   SNMP_VERSION              SNMP version: 1 | 2c | 3  (default: 2c)
#   SNMP_PORT                 UDP port  (default: 161)
#   SNMP_TIMEOUT              Request timeout in seconds  (default: 5)
#   SNMP_RETRIES              Retry count  (default: 1)
#   LINUX_SITE                NetBox site to place devices in (default: Default)
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

source "linux" {
  api_type   = "snmp"
  url        = "env('SNMP_HOSTS')"
  username   = "env('SNMP_COMMUNITY', 'public')"
  verify_ssl = false

  # These keys are passed through to source.extra
  version  = "env('SNMP_VERSION', '2c')"
  port     = "env('SNMP_PORT', '161')"
  timeout  = "env('SNMP_TIMEOUT', '5')"
  retries  = "env('SNMP_RETRIES', '1')"

  # NET-SNMP on Linux does not expose hardware serial or model via standard
  # MIBs.  If your snmpd is configured with nsExtend directives you can map
  # those OIDs to field names here.  The values are then available via
  # source('<field_name>') in the object block below.
  #
  # Example — dmidecode extensions (adjust the OID instance suffix to match
  # the exact byte-encoded command name used in your snmpd.conf):
  #
  # extra_oids = {
  #   hw_serial = "1.3.6.1.4.1.8072.1.3.2.4.1.2.6.115.101.114.105.97.108.1"
  #   hw_model  = "1.3.6.1.4.1.8072.1.3.2.4.1.2.5.109.111.100.101.108.1"
  # }
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
  site             = "env('LINUX_SITE', 'Default')"
  sync_interfaces  = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
}

# ---------------------------------------------------------------------------
# Linux Servers
#
# Vendor detection uses the sysObjectID OID prefix:
#   1.3.6.1.4.1.8072 — NET-SNMP (the standard agent for Linux/Unix)
#
# Kernel version is extracted from sysDescr, which has the format:
#   Linux <hostname> <kernel-version> <build-info> <arch>
#   e.g.: Linux myserver 5.15.0-91-generic #101-Ubuntu SMP … x86_64
#
# If extra_oids are configured (see source block above), the field
# source('hw_serial') and source('hw_model') carry those values.
# ---------------------------------------------------------------------------

object "device" {
  source_collection = "devices"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["name"]
  max_workers       = 4

  # --- prerequisites -------------------------------------------------------

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = {
      # Detect NET-SNMP Linux agent by its enterprise OID prefix.
      # Hardware manufacturer is not available via standard SNMP on Linux,
      # so 'Unknown' is used.  Extend or replace this expression if you
      # poll hosts from multiple vendors and can distinguish them by OID.
      name = "when('1.3.6.1.4.1.8072' in source('sys_object_id'), 'Unknown', source('manufacturer') or 'Unknown')"
    }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      # Use a vendor-supplied model name (via extra_oids) when available.
      # Fall back to a generic "Linux Server" label so every polled host
      # gets a device type even when hardware information is unavailable.
      model        = "source('hw_model') or 'Linux Server'"
      manufacturer = "prereq('manufacturer')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Server'" }
    optional = false
  }

  prerequisite "site" {
    method   = "ensure_site"
    args     = { name = "collector.site" }
    optional = false
  }

  prerequisite "platform" {
    method   = "ensure_platform"
    args     = {
      # Build a platform name from the kernel version embedded in sysDescr.
      # sysDescr format: Linux <hostname> <kernel-version> <build-info> <arch>
      # join() skips None values, so the result is just 'Linux' when the
      # kernel-version pattern is not found, and 'Linux <version>' when it is.
      name         = "join(' ', ['Linux', regex_extract(source('description'), '^Linux \\S+ (\\S+)')])"
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
    # Populated only when extra_oids are configured with a dmidecode extension.
    # Falls back to the generic serial field on the device dict (always "").
    value = "source('hw_serial') or source('serial')"
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
  #   label        — ifAlias (operator-assigned description)
  #   if_type      — raw SNMP ifType integer
  #   type         — standard ifType-mapped slug (may be "other" for Ethernet)
  #   mac_address  — formatted MAC address
  #   admin_status — "up" | "down" | "testing"
  #   speed        — speed in Mbps
  #   mtu          — MTU integer
  #
  # The interface type field uses regex_file() to apply Linux-specific
  # name-prefix mappings (see regex/linux-interface-types.csv), falling
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
      # Apply Linux interface-name → type mapping via regex_file.
      # The CSV returns an empty string for unrecognised names so that
      # `or` falls back to the standard ifType-mapped slug.
      value = "regex_file(lower(source('name')), 'linux-interface-types.csv') or source('type')"
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
