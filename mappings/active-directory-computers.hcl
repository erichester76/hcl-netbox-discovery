# Active Directory → NetBox Devices
#
# Queries Active Directory for computer accounts and creates/updates
# NetBox devices (dcim.devices) for each computer object found.
#
# Run this file to sync AD computers.  For user contacts, see
# mappings/active-directory-users.hcl.
#
# Required environment variables:
#   AD_SERVER             LDAP URI of the AD domain controller
#                         (e.g. ldaps://dc01.corp.example.com or
#                              ldap://dc01.corp.example.com:389)
#   AD_USER               Bind account DN or sAMAccountName@domain
#                         (e.g. CN=svc-netbox,OU=ServiceAccounts,DC=corp,DC=example,DC=com)
#   AD_PASS               Bind account password
#   AD_SEARCH_BASE        OU (or root) to search for computer objects
#                         (e.g. OU=Computers,DC=corp,DC=example,DC=com)
#   NETBOX_URL            NetBox base URL  (e.g. https://netbox.corp.example.com)
#   NETBOX_TOKEN          NetBox API token
#
# Optional:
#   AD_COMPUTERS_FILTER   LDAP search filter for computer accounts.
#                         Default: all computer objects
#                           (objectClass=computer)
#                         Examples:
#                           Windows servers only:
#                             (&(objectClass=computer)(operatingSystem=Windows Server*))
#                           Workstations only:
#                             (&(objectClass=computer)(operatingSystem=Windows 1*))
#   AD_DOMAIN             DNS domain suffix to strip from dNSHostName when
#                         constructing the device name
#                         (e.g. "corp.example.com").  Leave empty to use the
#                         full FQDN as the device name.
#   AD_DEFAULT_SITE       Name of the NetBox site to assign to every device.
#                         Created automatically if it does not exist.
#                         Default: "Default"
#   AD_DEFAULT_ROLE       Name of the NetBox device role to assign.
#                         Created automatically if it does not exist.
#                         Default: "Server"
#   AD_DEFAULT_MANUFACTURER
#                         Manufacturer name to use when building device types.
#                         Created automatically if it does not exist.
#                         Default: "Unknown"
#   NETBOX_CACHE_BACKEND  Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL      Redis URL or SQLite path when cache is redis/sqlite
#   DRY_RUN               Set to "true" to log payloads without writing to NetBox
#
# Active Directory attribute notes:
#   cn                   — Computer short name (sAMAccountName without the trailing $)
#   sAMAccountName       — SAM account name (computer name + "$" suffix)
#   dNSHostName          — Fully-qualified DNS hostname (e.g. pc01.corp.example.com)
#   operatingSystem      — OS name string (e.g. "Windows Server 2022 Standard")
#   operatingSystemVersion — OS version string (e.g. "10.0 (20348)")
#   description          — Free-text description field set by administrators
#   location             — Physical location (if set by AD administrators)
#   serialNumber         — Hardware serial (populated by some management tools)
#   distinguishedName    — Full LDAP DN; useful for debugging / comments

source "ldap" {
  api_type   = "ldap"
  url        = "env('AD_SERVER')"
  username   = "env('AD_USER')"
  password   = "env('AD_PASS')"
  verify_ssl = true

  search_base = "env('AD_SEARCH_BASE')"

  # Default filter: all computer objects
  search_filter = "env('AD_COMPUTERS_FILTER', '(objectClass=computer)')"

  # Fetch only the attributes needed for device records
  attributes = "cn,sAMAccountName,dNSHostName,operatingSystem,operatingSystemVersion,description,location,serialNumber,distinguishedName"
}

netbox {
  url        = "env('NETBOX_URL')"
  token      = "env('NETBOX_TOKEN')"
  cache      = "env('NETBOX_CACHE_BACKEND', 'none')"
  cache_url  = "env('NETBOX_CACHE_URL', '')"
  rate_limit = 0
}

collector {
  max_workers = 8
  dry_run     = "env('DRY_RUN', 'false')"
  sync_tag    = "ad-computers-sync"
  regex_dir   = "./regex"
}

# ---------------------------------------------------------------------------
# AD Computers → NetBox Devices
#
# Prerequisite chain
# ------------------
# manufacturer
#   A single catch-all manufacturer looked up from AD_DEFAULT_MANUFACTURER.
#   Set to a real vendor name (e.g. "Dell", "HP", "Lenovo") when your AD
#   is populated with accurate hardware data.
#
# device_type
#   Built from the operatingSystem attribute so that devices are grouped by
#   OS family.  The manufacturer resolved above is attached to the type.
#   Customise the model expression to use hardware-specific attributes if
#   your AD is enriched with SCCM / Intune hardware inventory data.
#
# role
#   A single role applied to all computers.  Change AD_DEFAULT_ROLE to
#   "Workstation", "Laptop", "Domain Controller", etc. as needed, or add
#   conditional logic with when() to derive the role from the OS string.
#
# site
#   A single default site applied to all computers.  Change AD_DEFAULT_SITE
#   or replace this prerequisite with a regex_file() lookup against a
#   site-mapping CSV if computers span multiple sites.
#
# platform
#   Created from the operatingSystem string so that NetBox surfaces OS
#   information in the device list.  Set optional = true so that devices
#   with no OS attribute are still synced.
#
# Field expression notes
# ----------------------
# name
#   Strip the AD_DOMAIN suffix from dNSHostName to get the short hostname.
#   Falls back to cn when dNSHostName is not set.
#   Truncated to 64 characters (NetBox device name limit).
#
# serial
#   Populated from the AD serialNumber attribute.  Most AD environments do
#   not populate this field; it is included here as a starting point for
#   organisations that synchronise hardware inventory into AD via SCCM,
#   Intune, or similar tools.
#
# comments
#   Combines the free-text description, physical location (if set), the OS
#   version string, and the full LDAP distinguished name so that engineers
#   can trace the record back to AD without leaving NetBox.
# ---------------------------------------------------------------------------

object "device" {
  source_collection = "computers"
  netbox_resource   = "dcim.devices"
  lookup_by         = ["name"]
  max_workers       = 8

  # Manufacturer — one catch-all; customise per site if needed
  prerequisite "manufacturer" {
    method   = "ensure_manufacturer"
    args     = { name = "env('AD_DEFAULT_MANUFACTURER', 'Unknown')" }
    optional = false
  }

  # Device type keyed by OS name — groups devices by operating system family
  prerequisite "device_type" {
    method   = "ensure_device_type"
    args     = {
      model        = "coalesce(source('operatingSystem'), 'Unknown OS')"
      manufacturer = "prereq('manufacturer')"
    }
    optional = false
  }

  # Role applied to all AD computers
  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "env('AD_DEFAULT_ROLE', 'Server')" }
    optional = false
  }

  # Default site — replace with a per-computer lookup when available
  prerequisite "site" {
    method   = "ensure_site"
    args     = { name = "env('AD_DEFAULT_SITE', 'Default')" }
    optional = false
  }

  # Platform from the OS name string
  prerequisite "platform" {
    method   = "ensure_platform"
    args     = { name = "coalesce(source('operatingSystem'), 'Unknown OS')" }
    optional = true
  }

  # Short hostname: strip domain suffix from FQDN; fall back to cn
  field "name" {
    value = "truncate(replace(source('dNSHostName') or source('cn'), '.' + env('AD_DOMAIN', ''), ''), 64)"
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

  field "status" {
    value = "'active'"
  }

  # Hardware serial number — populated only when AD has this data
  field "serial" {
    value = "source('serialNumber')"
  }

  # Human-readable notes combining description, location, OS version, and DN
  field "comments" {
    value = "join(' | ', [source('description'), source('location'), source('operatingSystemVersion'), source('distinguishedName')])"
  }

  field "tags" {
    type  = "tags"
    value = "['ad-computers-sync']"
  }
}
