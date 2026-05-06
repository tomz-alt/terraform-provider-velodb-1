# List all SaaS warehouses on AWS in us-east-1.
data "velodb_warehouses" "us_east_saas" {
  cloud_provider  = "aws"
  region          = "us-east-1"
  deployment_mode = "SaaS"
}

output "warehouse_count" {
  value = data.velodb_warehouses.us_east_saas.total
}

output "warehouse_names" {
  value = [for wh in data.velodb_warehouses.us_east_saas.warehouses : wh.name]
}
