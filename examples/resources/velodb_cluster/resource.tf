# On-demand cluster — always running, pay-as-you-go.
# `cache_gb` is omitted so the API auto-scales disk proportionally to compute_vcpu.
resource "velodb_cluster" "etl" {
  warehouse_id  = velodb_warehouse.saas.id
  name          = "compute-etl"
  cluster_type  = "COMPUTE"
  zone          = "us-east-1a"
  desired_state = "running"

  on_demand {
    compute_vcpu = 8
  }

  auto_pause {
    enabled              = true
    idle_timeout_minutes = 15
  }

  timeouts {
    create = "20m"
    update = "20m"
  }
}

# Paused dev cluster — cost-saving baseline.
resource "velodb_cluster" "dev" {
  warehouse_id  = velodb_warehouse.saas.id
  name          = "compute-dev"
  cluster_type  = "COMPUTE"
  zone          = "us-east-1a"
  desired_state = "paused"

  on_demand {
    compute_vcpu = 4
  }

  auto_pause {
    enabled              = true
    idle_timeout_minutes = 5
  }
}

# Prepaid (subscription) cluster.
resource "velodb_cluster" "prepaid" {
  warehouse_id  = velodb_warehouse.saas.id
  name          = "sql-primary"
  cluster_type  = "SQL"
  zone          = "us-east-1a"
  desired_state = "running"

  subscription {
    compute_vcpu = 16
    cache_gb     = 800
    period       = 1
    period_unit  = "Month"
    auto_renew   = true
  }

  auto_pause { enabled = false }
}

# Mixed-billing cluster: subscription baseline + on-demand burst capacity.
resource "velodb_cluster" "mixed" {
  warehouse_id = velodb_warehouse.saas.id
  name         = "compute-mixed"
  cluster_type = "COMPUTE"
  zone         = "us-east-1a"

  subscription {
    compute_vcpu = 16
    cache_gb     = 800
    period       = 1
    period_unit  = "Month"
    auto_renew   = false
  }
  on_demand {
    compute_vcpu = 8
  }
}

output "etl_endpoint" {
  value = velodb_cluster.etl.connection_info[0].public_endpoint
}
