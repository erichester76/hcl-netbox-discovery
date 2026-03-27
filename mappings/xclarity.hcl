# Lenovo XClarity Administrator → NetBox collector mapping
#
# Required environment variables:
#   XCLARITY_HOST     XClarity hostname or IP (no https://)
#   XCLARITY_USER     XClarity username
#   XCLARITY_PASS     XClarity password
#   NETBOX_URL        NetBox base URL
#   NETBOX_TOKEN      NetBox API token
#
# Optional:
#   XCLARITY_VERIFY_SSL       true | false  (default: true)
#   NETBOX_CACHE_BACKEND      Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL          Redis URL or SQLite path
#   DRY_RUN                   Set to "true" to log payloads without writing
#   COLLECTOR_SYNC_INTERFACES true | false  (default: true)
#   COLLECTOR_SYNC_INVENTORY  true | false  (default: true)

source "xclarity" {
  api_type   = "xclarity"
  url        = "env('XCLARITY_HOST')"
  username   = "env('XCLARITY_USER')"
  password   = "env('XCLARITY_PASS')"
  verify_ssl = "env('XCLARITY_VERIFY_SSL', 'true')"
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
  sync_tag         = "xclarity-sync"
  regex_dir        = "./regex"
  sync_interfaces  = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
  sync_inventory   = "env('COLLECTOR_SYNC_INVENTORY', 'true')"
}

# ---------------------------------------------------------------------------
# Compute Nodes (servers)
# ---------------------------------------------------------------------------

object "node" {
  source_collection = "nodes"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]
  max_workers       = 4

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "coalesce(source('machineType'), 'Lenovo')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce(source('productName'), source('machineType'), 'Unknown')"
      manufacturer = "prereq('manufacturer')"
      part_number  = "coalesce(source('partNumber'), '')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "Physical Server" }
    optional = false
  }

  prerequisite "site" {
    method   = "ensure_site"
    args     = { name = "coalesce(source('location.lowestRackUnit'), 'Unknown')" }
    optional = true
  }

  field "name" {
    value = "coalesce(source('hostname'), source('uuid'), 'Unknown')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "source('serialNumber')"
  }

  field "status" {
    value = "when(source('powerStatus') == 'on', 'active', 'offline')"
  }

  field "tags" {
    type  = "tags"
    value = "['xclarity-sync']"
  }

  # Network interfaces
  interface {
    source_items = "adapterSettings.onboardControllers[*].ports"
    enabled_if   = "collector.sync_interfaces"

    field "name" {
      value = "coalesce(source('portName'), source('physicalPortIndex'))"
    }

    field "mac_address" {
      value = "upper(source('address'))"
    }

    field "type" {
      value = "map_value(source('portSpeed'), {1: '1000base-t', 10: '10gbase-x-sfpp', 25: '25gbase-x-sfp28', 40: '40gbase-x-qsfpp', 100: '100gbase-x-qsfp28'}, 'other')"
    }
  }

  # CPUs
  inventory_item {
    source_items = "processors"
    role         = "CPU"
    enabled_if   = "collector.sync_inventory"
    dedupe_by    = "source('socket')"

    field "name" {
      value = "coalesce(source('socket'), source('productName'), 'CPU')"
    }

    field "part_id" {
      value = "coalesce(source('displayName'), source('partNumber'), '')"
    }

    field "serial" {
      value = "coalesce(source('serialNumber'), '')"
    }

    field "description" {
      value = "join(', ', [source('model'), str(source('speed'))])"
    }
  }

  # Memory DIMMs
  inventory_item {
    source_items = "memoryModules"
    role         = "Memory"
    enabled_if   = "collector.sync_inventory"
    dedupe_by    = "source('description')"

    field "name" {
      value = "coalesce(source('description'), source('partNumber'), 'DIMM')"
    }

    field "part_id" {
      value = "coalesce(source('partNumber'), '')"
    }

    field "serial" {
      value = "coalesce(source('serialNumber'), '')"
    }
  }

  # Hard Drives
  inventory_item {
    source_items = "storageSettings.raidSettings[*].diskDrives"
    role         = "Hard disk"
    enabled_if   = "collector.sync_inventory"
    dedupe_by    = "source('serialNumber')"

    field "name" {
      value = "coalesce(source('description'), source('partNumber'), 'Disk')"
    }

    field "part_id" {
      value = "coalesce(source('partNumber'), '')"
    }

    field "serial" {
      value = "coalesce(source('serialNumber'), '')"
    }

    field "description" {
      value = "join(' ', [source('diskType'), str(to_gb(source('capacity')))])"
    }
  }

  # Power Supplies
  inventory_item {
    source_items = "powerSupplySettings.powerSupplies"
    role         = "Power supply"
    enabled_if   = "collector.sync_inventory"
    dedupe_by    = "source('serialNumber')"

    field "name" {
      value = "coalesce(source('description'), source('partNumber'), 'PSU')"
    }

    field "part_id" {
      value = "coalesce(source('partNumber'), '')"
    }

    field "serial" {
      value = "coalesce(source('serialNumber'), '')"
    }
  }
}

# ---------------------------------------------------------------------------
# Chassis
# ---------------------------------------------------------------------------

object "chassis" {
  source_collection = "chassis"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "Lenovo" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce(source('productName'), source('machineType'), 'Unknown Chassis')"
      manufacturer = "prereq('manufacturer')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "Blade Chassis" }
    optional = false
  }

  field "name" {
    value = "coalesce(source('name'), source('uuid'), 'Unknown')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "source('serialNumber')"
  }

  field "status" {
    value = "'active'"
  }

  field "tags" {
    type  = "tags"
    value = "['xclarity-sync']"
  }
}
