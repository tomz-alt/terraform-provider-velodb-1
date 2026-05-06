# SaaS warehouse with initial cluster, automatic upgrade policy, and a UTC
# maintenance window.
resource "velodb_warehouse" "saas" {
  name            = "analytics-saas"
  deployment_mode = "SaaS"
  cloud_provider  = "aws"
  region          = "us-east-1"

  admin_password         = var.admin_password
  admin_password_version = 1

  upgrade_policy = "automatic"
  maintenance_window = {
    start_hour_utc = 2
    end_hour_utc   = 6
  }

  initial_cluster {
    name         = "default"
    zone         = "us-east-1a"
    compute_vcpu = 4
    cache_gb     = 1000
    auto_pause {
      enabled              = false
      idle_timeout_minutes = 30
    }
  }

  timeouts {
    create = "30m"
  }
}

# BYOC warehouse using guided setup. The provider returns CloudFormation /
# CloudShell links via the computed `byoc_setup` block.
resource "velodb_warehouse" "byoc" {
  name            = "production-byoc"
  deployment_mode = "BYOC"
  cloud_provider  = "aws"
  region          = "us-east-1"
  setup_mode      = "guided"
  vpc_mode        = "existing"
  vpc_id          = "vpc-0123456789abcdef0"

  admin_password         = var.admin_password
  admin_password_version = 1

  upgrade_policy = "automatic"
  maintenance_window = {
    start_hour_utc = 2
    end_hour_utc   = 6
  }

  initial_cluster {
    name         = "default-compute"
    zone         = "us-east-1a"
    compute_vcpu = 8
    cache_gb     = 400
    auto_pause {
      enabled              = true
      idle_timeout_minutes = 30
    }
  }

  timeouts {
    create = "45m"
  }
}

# Discover valid upgrade target version IDs for the SaaS warehouse.
# Reference `default_id` (or any specific `version_id`) on `core_version_id`
# in a follow-up apply to trigger an upgrade.
data "velodb_warehouse_versions" "saas_targets" {
  warehouse_id = velodb_warehouse.saas.id
}

output "byoc_shell_command" {
  value     = velodb_warehouse.byoc.byoc_setup[0].shell_command
  sensitive = true
}

output "saas_default_upgrade_id" {
  value = data.velodb_warehouse_versions.saas_targets.default_id
}

variable "admin_password" {
  type      = string
  sensitive = true
}
