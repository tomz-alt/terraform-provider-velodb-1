# Connection endpoints for a warehouse.
#
# The data source returns:
#   - public_connection         — list with 0 or 1 element (public access endpoint).
#   - private_inbound           — list with 0 or 1 element (PrivateLink inbound info).
#   - private_outbound_services — outbound PrivateLink services visible to this warehouse.
data "velodb_warehouse_connections" "prod" {
  warehouse_id = "ALBJ15F0"
}

locals {
  pub = try(data.velodb_warehouse_connections.prod.public_connection[0], null)
}

output "jdbc_url" {
  value = local.pub == null ? null : "jdbc:mysql://${local.pub.host}:${local.pub.jdbc_port}"
}

output "http_url" {
  value = local.pub == null ? null : "http://${local.pub.host}:${local.pub.http_port}"
}

output "stream_load_url" {
  value = local.pub == null ? null : "http://${local.pub.host}:${local.pub.stream_load_port}"
}

output "public_access_policy" {
  value = local.pub == null ? null : local.pub.public_access_policy
}

output "private_endpoint_service_name" {
  value = try(data.velodb_warehouse_connections.prod.private_inbound[0].endpoint_service_name, null)
}
