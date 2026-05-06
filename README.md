# Terraform Provider for VeloDB Cloud

Manages [VeloDB Cloud](https://www.velodb.cloud/) warehouses and clusters via the Management API. Built with the [Terraform Plugin Framework](https://github.com/hashicorp/terraform-plugin-framework).

## Resources & data sources

| Type | Name | Purpose |
|---|---|---|
| Resource | `velodb_warehouse` | SaaS or BYOC warehouse with maintenance window, upgrade policy, password rotation |
| Resource | `velodb_cluster` | Cluster with `subscription{}` / `on_demand{}` pool blocks (mixed billing supported) |
| Resource | `velodb_warehouse_public_access_policy` | IP allowlist (`DENY_ALL` / `ALLOW_ALL` / `ALLOWLIST_ONLY`) |
| Resource | `velodb_warehouse_private_endpoint` | Custom DNS for inbound PrivateLink endpoints |
| Resource | `velodb_private_link_endpoint_service` | Outbound PrivateLink service registration |
| Data source | `velodb_warehouses` | List warehouses with filters |
| Data source | `velodb_clusters` | List clusters in a warehouse |
| Data source | `velodb_warehouse_connections` | JDBC / HTTP / stream-load endpoints for a warehouse |
| Data source | `velodb_warehouse_versions` | Valid engine version IDs for upgrades |

Full reference: [`docs/`](docs/) (also rendered at the [Terraform Registry](https://registry.terraform.io/providers/velodb/velodb/latest/docs)). Worked examples: [`examples/`](examples/).

## Quickstart

```terraform
terraform {
  required_providers {
    velodb = {
      source  = "velodb/velodb"
      version = "~> 1.0"
    }
  }
}

provider "velodb" {
  host    = "sandbox.velodb.io"   # or VELODB_HOST env var
  api_key = var.api_key           # or VELODB_API_KEY env var
}

variable "api_key" {
  type      = string
  sensitive = true
}

resource "velodb_warehouse" "main" {
  name            = "analytics"
  deployment_mode = "SaaS"
  cloud_provider  = "aws"
  region          = "us-east-1"
  admin_password  = "ExamplePassword123!"

  upgrade_policy = "automatic"
  maintenance_window = {
    start_hour_utc = 2
    end_hour_utc   = 6
  }

  initial_cluster {
    name         = "default"
    zone         = "us-east-1a"
    compute_vcpu = 4
    cache_gb     = 100
    auto_pause { enabled = false }
  }
}

resource "velodb_cluster" "etl" {
  warehouse_id  = velodb_warehouse.main.id
  name          = "compute-etl"
  cluster_type  = "COMPUTE"
  zone          = "us-east-1a"
  desired_state = "running"

  on_demand {
    compute_vcpu = 8
    # cache_gb omitted — API auto-scales disk proportionally to compute_vcpu
  }

  auto_pause {
    enabled              = true
    idle_timeout_minutes = 15
  }
}
```

## Provider configuration

| Attribute | Type | Required | Sensitive | Description |
|---|---|---|---|---|
| `host` | String | No | No | Management API host. Falls back to `VELODB_HOST`. |
| `api_key` | String | No | Yes | API key. Falls back to `VELODB_API_KEY`. |

## Day-2 operations

| Op | How |
|---|---|
| Resize cluster | Change `compute_vcpu` and/or `cache_gb` inside the pool block — provider sequences the PATCHes (the API rejects combined CPU+disk changes). |
| Pause / resume | Set `desired_state = "paused"` / `"running"` — calls `POST /clusters/{id}/pause` or `/resume`. |
| Reboot | Bump `reboot_trigger` — calls `POST /clusters/{id}/reboot`. |
| Rotate password | Change `admin_password`, or bump `admin_password_version` to rotate without changing the value. |
| Upgrade engine | Set `core_version_id` to a value from the [`velodb_warehouse_versions`](docs/data-sources/warehouse_versions.md) data source. |
| Maintenance window | `maintenance_window = { start_hour_utc, end_hour_utc }` — UTC hours, validated `0–23` client-side. |

## Building from source

```bash
go install .
mkdir -p ~/.terraform.d
cat > ~/.terraformrc <<EOF
provider_installation {
  dev_overrides {
    "velodb/velodb" = "$(go env GOPATH)/bin"
  }
  direct {}
}
EOF
```

Now `terraform init` / `terraform apply` will use the locally built binary.

## Testing

- **Unit tests** (mock server, no API access): `go test ./...`
- **Live sandbox tests**: see [`test/sandbox/`](test/sandbox/README.md). Manual `workflow_dispatch` only — provisions real warehouses and incurs cost.
- **Test plan**: [`TEST_PLAN.md`](TEST_PLAN.md).

## Migrating from v0.x

The v1 release tracks the Management API spec (May 2026). Breaking HCL changes:

| v0.x | v1 |
|---|---|
| `maintainability_start_time = "02:00"` + `maintainability_end_time = "06:00"` | `maintenance_window = { start_hour_utc = 2, end_hour_utc = 6 }` |
| `core_version = "3.1.0"` (settable) | `core_version_id = data.velodb_warehouse_versions.x.default_id` (int64) |
| `advanced_settings = jsonencode({...})` | Removed — no replacement in the new API |
| `velodb_cluster { compute_vcpu = …, cache_gb = …, billing_method = … }` | `velodb_cluster { on_demand { compute_vcpu = … } }` (or `subscription{}`, or both) |

All four old fields fail at `terraform validate` in v1, so configurations break loudly rather than silently.

## License

Licensed under the [Mozilla Public License 2.0](LICENSE).
