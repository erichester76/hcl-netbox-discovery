# LDAP DHCP / static-lease → NetBox IP Address collector mapping
#
# Required environment variables:
#   LDAP_SERVER           LDAP server URI (e.g. ldaps://ldap.example.com)
#   LDAP_USER             Bind DN (e.g. cn=admin,dc=example,dc=com)
#   LDAP_PASS             Bind password
#   LDAP_SEARCH_BASE      LDAP search base (e.g. ou=Network Devices,o=example)
#   NETBOX_URL            NetBox base URL
#   NETBOX_TOKEN          NetBox API token
#
# Optional:
#   LDAP_FILTER               LDAP search filter
#                             (default: "(DirXMLjnsuDHCPAddress=*)")
#   LDAP_SKIP_APS             Skip access-point entries: true | false (default: true)
#   LDAP_PREFIX_LENGTH        Default prefix length appended to IPs (e.g. "24")
#                             Leave empty to store bare IPs as /32 in NetBox.
#   NETBOX_CACHE_BACKEND      Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL          Redis URL or SQLite path
#   DRY_RUN                   Set to "true" to log payloads without writing

source "ldap" {
  api_type   = "ldap"
  url        = "env('LDAP_SERVER')"
  username   = "env('LDAP_USER')"
  password   = "env('LDAP_PASS')"
  verify_ssl = true

  # Extra source-specific configuration
  search_base            = "env('LDAP_SEARCH_BASE')"
  search_filter          = "env('LDAP_FILTER', '(DirXMLjnsuDHCPAddress=*)')"
  skip_aps               = "env('LDAP_SKIP_APS', 'true')"
  default_prefix_length  = "env('LDAP_PREFIX_LENGTH', '')"
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
  sync_tag    = "ldap-sync"
  regex_dir   = "./regex"
}

# ---------------------------------------------------------------------------
# IP Addresses (DHCP leases and static registrations)
#
# The source adapter pre-normalises description formatting, MAC extraction,
# and AP filtering so that the HCL can stay simple.
# ---------------------------------------------------------------------------

object "ip_address" {
  source_collection = "dhcp_leases"
  netbox_resource   = "ipam.ip-addresses"
  lookup_by         = ["address"]
  max_workers       = 8

  field "address" {
    value = "source('address')"
  }

  field "description" {
    value = "source('description')"
  }

  field "status" {
    value = "source('status')"
  }

  field "tags" {
    type  = "tags"
    value = "['ldap-sync']"
  }
}
