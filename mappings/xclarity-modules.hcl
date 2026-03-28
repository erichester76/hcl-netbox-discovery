# Lenovo XClarity Administrator → NetBox collector mapping (modules variant)
#
# This file is a companion to xclarity.hcl.  Where xclarity.hcl uses
# inventory_item blocks to record CPUs, memory, drives, add-in cards, power
# supplies, and fans as NetBox InventoryItems, this file uses module blocks so
# that each hardware component is modelled as a proper NetBox Module installed
# in a ModuleBay on the device.
#
# The modules approach maps more closely to physical reality:
#   ModuleBayTemplate  — declares that a slot exists on the DeviceType
#   ModuleBay          — the physical slot instance on a Device
#   ModuleType         — the make/model of an installed component (reusable)
#   Module             — the installed instance (device + bay + type + serial)
#
# Required environment variables:
#   XCLARITY_HOST       XClarity hostname, IP, or full URL
#   XCLARITY_USER       XClarity username
#   XCLARITY_PASS       XClarity password
#   NETBOX_URL          NetBox base URL
#   NETBOX_TOKEN        NetBox API token
#
# Optional:
#   XCLARITY_VERIFY_SSL       true | false  (default: true)
#   NETBOX_CACHE_BACKEND      Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL          Redis URL or SQLite path
#   DRY_RUN                   Set to "true" to log payloads without writing
#   COLLECTOR_SYNC_INTERFACES true | false  (default: true)
#   COLLECTOR_SYNC_MODULES    true | false  (default: true)
#
# XClarity location mapping:
#   location.location      → NetBox site
#   location.room          → NetBox location (area within site)
#   location.rack          → NetBox rack
#   location.lowestRackUnit → rack position (front face, U number)

source "xclarity" {
  api_type   = "rest"
  url        = "env('XCLARITY_HOST')"
  username   = "env('XCLARITY_USER')"
  password   = "env('XCLARITY_PASS')"
  verify_ssl = "env('XCLARITY_VERIFY_SSL', 'true')"
  auth       = "basic"

  collection "nodes" {
    endpoint        = "/nodes"
    list_key        = "nodeList"
    detail_endpoint = "/nodes/{uuid}"
    detail_id_field = "uuid"
  }

  collection "chassis" {
    endpoint        = "/chassis"
    list_key        = "chassisList"
    detail_endpoint = "/chassis/{uuid}"
    detail_id_field = "uuid"
  }

  collection "switches" {
    endpoint = "/switches"
    list_key = "switchList"
  }

  collection "storage" {
    endpoint = "/storage"
    list_key = "storageList"
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
  sync_tag         = "xclarity-sync"
  regex_dir        = "./regex"
  sync_interfaces  = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
  sync_modules     = "env('COLLECTOR_SYNC_MODULES', 'true')"
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
    args     = { name = "when(source('manufacturer') != None, source('manufacturer'), 'Lenovo')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce('productName', 'machineType', 'model')"
      manufacturer = "prereq('manufacturer')"
      part_number  = "source('partNumber')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Physical Server'" }
    optional = false
  }

  prerequisite "placement" {
    method   = "resolve_placement"
    args     = {
      site     = "regex_file(coalesce('location.location', 'dataCenter'), 'xclarity_location_to_site')"
      location = "regex_file(source('location.room'), 'xclarity_room_to_location')"
      rack     = "source('location.rack')"
      position = "source('location.lowestRackUnit')"
    }
    optional = true
  }

  field "name" {
    value = "regex_replace(coalesce('name', 'hostname'), '-sp.*$', '')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "str(source('serialNumber'))"
  }

  field "site" {
    value = "prereq('placement.site_id')"
  }

  field "location" {
    value = "prereq('placement.location_id')"
  }

  field "rack" {
    value = "prereq('placement.rack_id')"
  }

  field "position" {
    value = "prereq('placement.rack_position')"
  }

  field "face" {
    value = "when(prereq('placement.rack_id') != None, 'front', None)"
  }

  field "status" {
    value = "when(source('powerStatus') == 'on', 'active', 'offline')"
  }

  # Network interfaces — sourced from onboard controller ports reported by
  # XClarity.  portSpeed is in Gbps (integer).
  interface {
    source_items = "adapterSettings.onboardControllers[*].ports"
    enabled_if   = "collector.sync_interfaces"

    field "name" {
      value = "coalesce('portName', 'physicalPortIndex')"
    }

    field "mac_address" {
      value = "upper(source('address'))"
    }

    field "type" {
      value = "map_value(source('portSpeed'), {1: '1000base-t', 10: '10gbase-t', 25: '25gbase-x-sfp28', 40: '40gbase-x-qsfpp', 100: '100gbase-x-qsfp28'}, 'other')"
    }
  }

  # ---------------------------------------------------------------------------
  # Hardware modules — each block corresponds to one component category.
  # The engine ensures the ModuleBayTemplate → ModuleBay → ModuleType →
  # Module chain for every item.
  #
  # Fields used by the module processor (not written to NetBox directly):
  #   bay_name     — unique slot label on the device (e.g. "CPU Socket 1")
  #   position     — numeric position within the device type template
  #   model        — identifies the ModuleType (make/model string)
  #   serial       — installed module serial number
  #   manufacturer — manufacturer name (looked up / created automatically)
  # ---------------------------------------------------------------------------

  # CPUs
  module {
    source_items = "processors"
    profile      = "CPU"
    enabled_if   = "collector.sync_modules"
    dedupe_by    = "source('socket')"

    field "bay_name" {
      value = "coalesce('socket', 'productName', 'description')"
    }

    field "position" {
      value = "str(source('slot'))"
    }

    field "model" {
      value = "coalesce('displayName', 'productVersion', 'model')"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
    }
  }

  # Memory DIMMs
  module {
    source_items = "memoryModules"
    profile      = "Memory"
    enabled_if   = "collector.sync_modules"

    field "bay_name" {
      value = "coalesce('displayName', 'productName', 'description')"
    }

    field "position" {
      value = "str(source('slot'))"
    }

    field "model" {
      value = "coalesce('partNumber', 'description')"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
    }
  }

  # Hard Drives — raidSettings is the top-level array; each entry has a
  # diskDrives list.  The [*] wildcard flattens all drives from all controllers.
  module {
    source_items = "raidSettings[*].diskDrives"
    profile      = "Hard disk"
    enabled_if   = "collector.sync_modules"
    dedupe_by    = "source('serialNumber')"

    field "bay_name" {
      value = "str(coalesce('name', 'description'))"
    }

    field "position" {
      value = "str(coalesce('bay', 'slot'))"
    }

    field "model" {
      value = "coalesce('model', 'partNumber', 'description')"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
    }
  }

  # PCIe add-in cards
  module {
    source_items = "addinCards"
    profile      = "Expansion card"
    enabled_if   = "collector.sync_modules"

    field "bay_name" {
      value = "coalesce('slotName', 'productName', 'name')"
    }

    field "position" {
      value = "str(coalesce('slotNumber', 'slot'))"
    }

    field "model" {
      value = "coalesce('productName', 'name', 'description')"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
    }
  }

  # Power Supplies
  module {
    source_items = "powerSupplies"
    profile      = "Power supply"
    enabled_if   = "collector.sync_modules"
    dedupe_by    = "source('serialNumber')"

    field "bay_name" {
      value = "coalesce('name', 'description')"
    }

    field "position" {
      value = "str(source('slot'))"
    }

    field "model" {
      value = "coalesce('partNumber', 'model', 'productName')"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
    }
  }

  # Fans
  module {
    source_items = "fans"
    profile      = "Fan"
    enabled_if   = "collector.sync_modules"

    field "bay_name" {
      value = "coalesce('name', 'description')"
    }

    field "position" {
      value = "str(source('slot'))"
    }

    field "model" {
      value = "coalesce('partNumber', 'model', str("'Fan'"))"
    }

    field "serial" {
      value = "str(source('serialNumber'))"
    }

    field "manufacturer" {
      value = "source('manufacturer')"
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
    args     = { name = "when(source('manufacturer') != None, source('manufacturer'), 'Lenovo')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce('productName', 'machineType')"
      manufacturer = "prereq('manufacturer')"
      part_number  = "source('partNumber')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Blade Chassis'" }
    optional = false
  }

  prerequisite "placement" {
    method   = "resolve_placement"
    args     = {
      site     = "regex_file(coalesce('location.location', 'dataCenter'), 'xclarity_location_to_site')"
      location = "regex_file(source('location.room'), 'xclarity_room_to_location')"
      rack     = "source('location.rack')"
      position = "source('location.lowestRackUnit')"
    }
    optional = true
  }

  field "name" {
    value = "regex_replace(coalesce('name', 'hostname'), '-sp.*$', '')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "str(source('serialNumber'))"
  }

  field "site" {
    value = "prereq('placement.site_id')"
  }

  field "location" {
    value = "prereq('placement.location_id')"
  }

  field "rack" {
    value = "prereq('placement.rack_id')"
  }

  field "position" {
    value = "prereq('placement.rack_position')"
  }

  field "face" {
    value = "when(prereq('placement.rack_id') != None, 'front', None)"
  }

  field "status" {
    value = "'active'"
  }

}

# ---------------------------------------------------------------------------
# Switches
# ---------------------------------------------------------------------------

object "switch" {
  source_collection = "switches"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "when(source('manufacturer') != None, source('manufacturer'), 'Lenovo')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce('productName', 'machineType', 'model')"
      manufacturer = "prereq('manufacturer')"
      part_number  = "source('partNumber')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Switch'" }
    optional = false
  }

  prerequisite "placement" {
    method   = "resolve_placement"
    args     = {
      site     = "regex_file(coalesce('location.location', 'dataCenter'), 'xclarity_location_to_site')"
      location = "regex_file(source('location.room'), 'xclarity_room_to_location')"
      rack     = "source('location.rack')"
      position = "source('location.lowestRackUnit')"
    }
    optional = true
  }

  field "name" {
    value = "regex_replace(coalesce('name', 'hostname'), '-sp.*$', '')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "str(source('serialNumber'))"
  }

  field "site" {
    value = "prereq('placement.site_id')"
  }

  field "location" {
    value = "prereq('placement.location_id')"
  }

  field "rack" {
    value = "prereq('placement.rack_id')"
  }

  field "position" {
    value = "prereq('placement.rack_position')"
  }

  field "face" {
    value = "when(prereq('placement.rack_id') != None, 'front', None)"
  }

  field "status" {
    value = "'active'"
  }

  # Switch ports
  interface {
    source_items = "portList"
    enabled_if   = "collector.sync_interfaces"

    field "name" {
      value = "coalesce('portName', 'name')"
    }

    field "mac_address" {
      value = "upper(coalesce('macAddress', 'mac'))"
    }

    field "type" {
      value = "map_value(source('portSpeed'), {1: '1000base-t', 10: '10gbase-t', 25: '25gbase-x-sfp28', 40: '40gbase-x-qsfpp', 100: '100gbase-x-qsfp28'}, 'other')"
    }
  }
}

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

object "storage" {
  source_collection = "storage"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["serial"]

  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "when(source('manufacturer') != None, source('manufacturer'), 'Lenovo')" }
    optional = false
  }

  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce('productName', 'machineType', 'model')"
      manufacturer = "prereq('manufacturer')"
      part_number  = "source('partNumber')"
    }
    optional = false
  }

  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "'Storage'" }
    optional = false
  }

  prerequisite "placement" {
    method   = "resolve_placement"
    args     = {
      site     = "regex_file(coalesce('location.location', 'dataCenter'), 'xclarity_location_to_site')"
      location = "regex_file(source('location.room'), 'xclarity_room_to_location')"
      rack     = "source('location.rack')"
      position = "source('location.lowestRackUnit')"
    }
    optional = true
  }

  field "name" {
    value = "regex_replace(coalesce('name', 'hostname'), '-sp.*$', '')"
  }

  field "device_type" {
    value = "prereq('device_type')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "serial" {
    value = "str(source('serialNumber'))"
  }

  field "site" {
    value = "prereq('placement.site_id')"
  }

  field "location" {
    value = "prereq('placement.location_id')"
  }

  field "rack" {
    value = "prereq('placement.rack_id')"
  }

  field "position" {
    value = "prereq('placement.rack_position')"
  }

  field "face" {
    value = "when(prereq('placement.rack_id') != None, 'front', None)"
  }

  field "status" {
    value = "'active'"
  }

}
