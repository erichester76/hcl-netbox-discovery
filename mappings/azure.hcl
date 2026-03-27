# Microsoft Azure → NetBox collector mapping
#
# Required environment variables (service principal auth):
#   AZURE_TENANT_ID       Azure AD tenant ID
#   AZURE_CLIENT_ID       Service principal client (app) ID
#   AZURE_CLIENT_SECRET   Service principal secret
#   NETBOX_URL            NetBox base URL
#   NETBOX_TOKEN          NetBox API token
#
# Alternatively set AZURE_USE_DEFAULT_CRED=true to use DefaultAzureCredential
# (works with az login, managed identities, environment credentials, etc.)
#
# Optional:
#   AZURE_SUBSCRIPTION_IDS  Comma-separated subscription IDs to limit scope
#   NETBOX_CACHE_BACKEND    Cache backend: none | redis | sqlite  (default: none)
#   NETBOX_CACHE_URL        Redis URL or SQLite path
#   DRY_RUN                 Set to "true" to log payloads without writing
#   COLLECTOR_SYNC_INTERFACES  true | false  (default: true)
#   COLLECTOR_SYNC_DISKS       true | false  (default: true)

source "azure" {
  api_type   = "azure"
  url        = ""          # not used for Azure; auth is via credentials below
  username   = "env('AZURE_CLIENT_ID', '')"
  password   = "env('AZURE_CLIENT_SECRET', '')"
  verify_ssl = true

  # Set auth_method = "service_principal" to use client_id / client_secret.
  # Omit (or set to "default") to use DefaultAzureCredential.
  auth_method = "env('AZURE_AUTH_METHOD', 'default')"
  tenant_id   = "env('AZURE_TENANT_ID', '')"
}

netbox {
  url        = "env('NETBOX_URL')"
  token      = "env('NETBOX_TOKEN')"
  cache      = "env('NETBOX_CACHE_BACKEND', 'none')"
  cache_url  = "env('NETBOX_CACHE_URL', '')"
  rate_limit = 0
}

collector {
  max_workers       = 4
  dry_run           = "env('DRY_RUN', 'false')"
  sync_tag          = "azure-sync"
  regex_dir         = "./regex"
  sync_interfaces   = "env('COLLECTOR_SYNC_INTERFACES', 'true')"
  sync_disks        = "env('COLLECTOR_SYNC_DISKS', 'true')"
}

# ---------------------------------------------------------------------------
# IP Prefixes (VNet address spaces and subnets)
# ---------------------------------------------------------------------------

object "prefix" {
  source_collection = "prefixes"
  netbox_resource   = "ipam.prefixes"
  lookup_by         = ["prefix"]
  max_workers       = 4

  prerequisite "tenant" {
    method   = "ensure_tenant"
    args     = {
      name        = "source('subscription_name')"
      description = "join('', ['Azure Subscription: ', source('subscription_id')])"
    }
    optional = true
  }

  field "prefix" {
    value = "source('prefix')"
  }

  field "description" {
    value = "source('description')"
  }

  field "status" {
    value = "'active'"
  }

  field "tenant" {
    value = "prereq('tenant')"
  }

  field "tags" {
    type  = "tags"
    value = "['azure-sync']"
  }
}

# ---------------------------------------------------------------------------
# Virtual Machines
# ---------------------------------------------------------------------------

object "vm" {
  source_collection = "vms"
  netbox_resource   = "virtualization.virtual_machines"
  lookup_by         = ["name"]
  max_workers       = 4

  # Tenant per Azure subscription
  prerequisite "tenant" {
    method   = "ensure_tenant"
    args     = {
      name        = "source('subscription_name')"
      description = "join('', ['Azure Subscription: ', source('subscription_id')])"
    }
    optional = true
  }

  # Cluster per Azure region
  prerequisite "cluster_type" {
    method   = "ensure_cluster_type"
    args     = { name = "Azure" }
    optional = false
  }

  prerequisite "cluster" {
    method   = "ensure_cluster"
    args     = {
      name = "source('cluster_name')"
      type = "prereq('cluster_type')"
    }
    optional = false
  }

  # Platform (OS image)
  prerequisite "platform" {
    method   = "ensure_platform"
    args     = { name = "coalesce(source('platform_name'), 'Unknown')" }
    optional = true
  }

  # Role
  prerequisite "role" {
    method   = "ensure_device_role"
    args     = { name = "Azure VM" }
    optional = false
  }

  field "name" {
    value = "source('name')"
  }

  field "status" {
    value = "source('status')"
  }

  field "cluster" {
    value = "prereq('cluster')"
  }

  field "role" {
    value = "prereq('role')"
  }

  field "tenant" {
    value = "prereq('tenant')"
  }

  field "platform" {
    value = "prereq('platform')"
  }

  field "vcpus" {
    value = "source('vcpus')"
  }

  field "memory" {
    value = "source('memory')"
  }

  field "tags" {
    type  = "tags"
    value = "['azure-sync']"
  }

  # Network interfaces
  interface {
    source_items = "nics"
    enabled_if   = "collector.sync_interfaces"

    field "name" {
      value = "source('name')"
    }

    field "mac_address" {
      value = "upper(source('mac_address'))"
    }

    field "type" {
      value = "'virtual'"
    }

    ip_address {
      source_items = "ips"
      primary_if   = "first"

      field "address" {
        value = "source('address')"
      }

      field "status" {
        value = "'active'"
      }
    }
  }

  # Virtual disks
  disk {
    source_items = "disks"
    enabled_if   = "collector.sync_disks"

    field "name" {
      value = "source('name')"
    }

    field "size" {
      value = "source('size_mb')"
    }
  }
}
