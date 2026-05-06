# List valid upgrade target versions for a warehouse.
#
# The Management API requires `targetVersionId` (int64) on the upgrade
# endpoint, so this data source is the source of truth for which versions
# are upgrade-eligible right now.
#
# Usage pattern (two-apply):
#   1. First apply creates the warehouse without `core_version_id`.
#   2. After the warehouse is Running, the data source can resolve
#      `default_id`. Add `core_version_id` to the resource and apply
#      again to trigger an upgrade.
#
# Setting `core_version_id = data.velodb_warehouse_versions.x.default_id`
# at warehouse creation time creates a cycle (the data source depends
# on the warehouse ID), so do not wire it up in the same apply.
data "velodb_warehouse_versions" "available" {
  warehouse_id = velodb_warehouse.main.id
}

resource "velodb_warehouse" "main" {
  name            = "analytics"
  deployment_mode = "SaaS"
  cloud_provider  = "aws"
  region          = "us-east-1"
  admin_password  = "ExamplePassword123!"

  # Add `core_version_id` here in a follow-up apply once the data source
  # has populated. The provider rejects core_version_id <= 0 with a
  # helpful error, so guarding for empty results isn't required.
  # core_version_id = data.velodb_warehouse_versions.available.default_id

  initial_cluster {
    name         = "default"
    zone         = "us-east-1a"
    compute_vcpu = 4
    cache_gb     = 100
    auto_pause { enabled = false }
  }
}

output "default_target_id" {
  value = data.velodb_warehouse_versions.available.default_id
}

output "all_versions" {
  value = data.velodb_warehouse_versions.available.versions
}
