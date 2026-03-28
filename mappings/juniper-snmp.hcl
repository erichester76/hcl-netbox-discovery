# Juniper router SNMP → NetBox collector mapping
#
# Polls one or more Juniper routers via SNMP and syncs device records,
# interfaces and IP addresses into NetBox.
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
# The SNMP source adapter normalises Juniper-specific OIDs so that the
# mapping below works with standard field expressions.
# ---------------------------------------------------------------------------

object "device" {
  source_collection = "devices"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]
  max_workers       = 4

  # --- prerequisites -------------------------------------------------------

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "coalesce(source('manufacturer'), 'Juniper Networks')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce(source('model'), 'Unknown')"
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
      name         = "coalesce(source('platform'), 'Junos')"
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
    value = "source('serial')"
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
  # The source adapter returns ifName (with ifAlias as label), the NetBox
  # type slug determined from the interface name/ifType, and the admin/oper
  # status booleans.
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
      value = "source('type')"
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
