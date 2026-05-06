terraform {
  required_providers {
    velodb = {
      source  = "velodb/velodb"
      version = "~> 1.0"
    }
  }
}

provider "velodb" {
  host    = var.velodb_host
  api_key = var.velodb_api_key
}

variable "velodb_host" {
  type        = string
  description = "VeloDB Management API host"
  default     = "sandbox.velodb.io"
}

variable "velodb_api_key" {
  type        = string
  description = "VeloDB API key"
  sensitive   = true
}
