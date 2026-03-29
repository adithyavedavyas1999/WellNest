# variables.tf — shared variable definitions for WellNest infrastructure
#
# These get referenced by both the AWS and Azure modules.
# Override them via terraform.tfvars or -var flags.

variable "project" {
  description = "Project name, used as a prefix for resource naming"
  type        = string
  default     = "wellnest"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod"
  }
}

variable "region" {
  description = "Primary cloud region for resource deployment"
  type        = string
  default     = "us-east-1"
}

variable "db_name" {
  description = "Name of the application database"
  type        = string
  default     = "wellnest"
}

variable "db_username" {
  description = "Master username for the database"
  type        = string
  default     = "wellnest"
  sensitive   = true
}

variable "db_password" {
  description = "Master password for the database — pass this via TF_VAR_db_password, not in tfvars"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS / Azure SQL compute tier"
  type        = string
  default     = "db.t4g.micro"
}

variable "enable_deletion_protection" {
  description = "Prevent accidental resource deletion (flip to true for prod)"
  type        = bool
  default     = false
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to hit the database directly (VPN, office IP, etc.)"
  type        = list(string)
  default     = []
}

variable "api_container_image" {
  description = "Docker image URI for the FastAPI service"
  type        = string
  default     = ""
}

variable "domain_name" {
  description = "Custom domain for the API / dashboard (leave empty to skip DNS setup)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Extra tags to slap on every resource"
  type        = map(string)
  default     = {}
}

# computed locals that every module can use
locals {
  name_prefix = "${var.project}-${var.environment}"

  default_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
    },
    var.tags,
  )
}
