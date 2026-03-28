# Cisco Catalyst Center → NetBox collector mapping
#
# Required environment variables:
#   CATC_HOST         Catalyst Center hostname or IP (no https://)
#   CATC_USER         Catalyst Center username
#   CATC_PASS         Catalyst Center password
#   NETBOX_URL        NetBox base URL
#   NETBOX_TOKEN      NetBox API token
#
# Optional:
#   CATC_VERIFY_SSL           true | false  (default: true)
#   NETBOX_CACHE_BACKEND      Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL          Redis URL or SQLite path
#   DRY_RUN                   Set to "true" to log payloads without writing
#   COLLECTOR_SYNC_INTERFACES true | false  (default: true)
#   COLLECTOR_SYNC_INVENTORY  true | false  (default: true)

source "catc" {
  api_type   = "catc"
  url        = "env('CATC_HOST')"
  username   = "env('CATC_USER')"
  password   = "env('CATC_PASS')"
  verify_ssl = "env('CATC_VERIFY_SSL', 'true')"
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
  sync_tag         = "catc-sync"
  regex_dir        = "./regex"
  sync_interfaces  = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
  sync_inventory   = "env('COLLECTOR_SYNC_INVENTORY', 'true')"
}

# ---------------------------------------------------------------------------
# Network Devices
#
# The source adapter pre-normalises all Cisco-specific model strings and site
# hierarchy extraction so that HCL expressions stay simple.
# ---------------------------------------------------------------------------

object "device" {
  source_collection = "devices"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]
  max_workers       = 4

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "source('manufacturer')" }
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
    args     = { name = "coalesce(source('role'), 'Network Device')" }
    optional = false
  }

  prerequisite "site" {
    method   = "ensure_site"
    args     = { name = "coalesce(source('site_name'), 'Unknown')" }
    optional = false
  }

  prerequisite "platform" {
    method   = "ensure_platform"
    args     = {
      name         = "coalesce(source('platform_name'), 'Unknown')"
      manufacturer = "prereq('manufacturer')"
    }
    optional = true
  }

  field "name" {
    value = "coalesce(source('name'), 'Unknown')"
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

  field "status" {
    value = "source('status')"
  }

  field "tags" {
    type  = "tags"
    value = "['catc-sync']"
  }
}
