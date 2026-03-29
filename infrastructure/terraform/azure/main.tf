# azure/main.tf — WellNest infrastructure on Azure
#
# What we're deploying:
#   - Resource Group (everything lives here)
#   - Azure Database for PostgreSQL Flexible Server
#   - App Service (Linux) for the FastAPI backend
#   - Storage Account + Blob container for the data lake
#   - Application Insights for observability
#
# We're using Azure's flexible server because it supports PostGIS
# out of the box and the pricing is way more predictable than the
# single server tier they're deprecating.

terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }

  # backend "azurerm" {
  #   resource_group_name  = "wellnest-terraform"
  #   storage_account_name = "wellnesttfstate"
  #   container_name       = "tfstate"
  #   key                  = "azure/terraform.tfstate"
  # }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# we need the Azure-specific region variable to override the AWS default
variable "azure_region" {
  description = "Azure region — defaults to East US to stay close to Chicago"
  type        = string
  default     = "eastus"
}

variable "azure_sku_name" {
  description = "SKU for the PostgreSQL flexible server"
  type        = string
  default     = "B_Standard_B1ms"
}

variable "app_service_sku" {
  description = "App Service Plan SKU"
  type        = string
  default     = "B1"
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------

resource "azurerm_resource_group" "main" {
  name     = "${local.name_prefix}-rg"
  location = var.azure_region
  tags     = local.default_tags
}

# ---------------------------------------------------------------------------
# Virtual Network
# ---------------------------------------------------------------------------

resource "azurerm_virtual_network" "main" {
  name                = "${local.name_prefix}-vnet"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = ["10.0.0.0/16"]
  tags                = local.default_tags
}

resource "azurerm_subnet" "app" {
  name                 = "${local.name_prefix}-app-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "app-service-delegation"
    service_delegation {
      name = "Microsoft.Web/serverFarms"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/action",
      ]
    }
  }
}

resource "azurerm_subnet" "db" {
  name                 = "${local.name_prefix}-db-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "postgres-delegation"
    service_delegation {
      name = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
      ]
    }
  }
}

resource "azurerm_private_dns_zone" "postgres" {
  name                = "${local.name_prefix}.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.default_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres" {
  name                  = "${local.name_prefix}-pg-dns-link"
  private_dns_zone_name = azurerm_private_dns_zone.postgres.name
  virtual_network_id    = azurerm_virtual_network.main.id
  resource_group_name   = azurerm_resource_group.main.name
}

# ---------------------------------------------------------------------------
# PostgreSQL Flexible Server
# ---------------------------------------------------------------------------

resource "azurerm_postgresql_flexible_server" "main" {
  name                = "${local.name_prefix}-pgflex"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  version    = "16"
  sku_name   = var.azure_sku_name
  storage_mb = 32768

  delegated_subnet_id = azurerm_subnet.db.id
  private_dns_zone_id = azurerm_private_dns_zone.postgres.id

  administrator_login    = var.db_username
  administrator_password = var.db_password

  backup_retention_days        = var.environment == "prod" ? 14 : 7
  geo_redundant_backup_enabled = var.environment == "prod"

  zone = "1"

  tags = local.default_tags

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

resource "azurerm_postgresql_flexible_server_database" "wellnest" {
  name      = var.db_name
  server_id = azurerm_postgresql_flexible_server.main.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_postgresql_flexible_server_database" "dagster" {
  name      = "dagster"
  server_id = azurerm_postgresql_flexible_server.main.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# enable PostGIS — azure exposes this as a server configuration
resource "azurerm_postgresql_flexible_server_configuration" "extensions" {
  name      = "azure.extensions"
  server_id = azurerm_postgresql_flexible_server.main.id
  value     = "POSTGIS,UUID-OSSP,PG_TRGM"
}

resource "azurerm_postgresql_flexible_server_configuration" "log_duration" {
  name      = "log_min_duration_statement"
  server_id = azurerm_postgresql_flexible_server.main.id
  value     = "1000"
}

# ---------------------------------------------------------------------------
# Storage Account — data lake (Blob)
# ---------------------------------------------------------------------------

resource "azurerm_storage_account" "data_lake" {
  name                     = replace("${var.project}${var.environment}lake", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.environment == "prod" ? "GRS" : "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  min_tls_version = "TLS1_2"

  blob_properties {
    versioning_enabled = true

    delete_retention_policy {
      days = 30
    }
  }

  tags = local.default_tags
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed" {
  name                  = "processed"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "models" {
  name                  = "models"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

# ---------------------------------------------------------------------------
# App Service — FastAPI backend
# ---------------------------------------------------------------------------

resource "azurerm_service_plan" "api" {
  name                = "${local.name_prefix}-plan"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  sku_name            = var.app_service_sku
  tags                = local.default_tags
}

resource "azurerm_linux_web_app" "api" {
  name                      = "${local.name_prefix}-api"
  location                  = azurerm_resource_group.main.location
  resource_group_name       = azurerm_resource_group.main.name
  service_plan_id           = azurerm_service_plan.api.id
  https_only                = true
  virtual_network_subnet_id = azurerm_subnet.app.id

  site_config {
    always_on = var.environment == "prod"

    application_stack {
      docker_image_name = var.api_container_image != "" ? var.api_container_image : "python:3.11-slim"
    }

    health_check_path                 = "/health"
    health_check_eviction_time_in_min = 5

    cors {
      allowed_origins = ["*"]
    }
  }

  app_settings = {
    DATABASE_URL                    = "postgresql://${var.db_username}:${var.db_password}@${azurerm_postgresql_flexible_server.main.fqdn}:5432/${var.db_name}?sslmode=require"
    ENVIRONMENT                     = var.environment
    LOG_LEVEL                       = var.environment == "prod" ? "WARNING" : "INFO"
    APPINSIGHTS_INSTRUMENTATIONKEY  = azurerm_application_insights.main.instrumentation_key
    APPLICATIONINSIGHTS_CONNECTION_STRING = azurerm_application_insights.main.connection_string
    WEBSITES_ENABLE_APP_SERVICE_STORAGE  = "false"
  }

  logs {
    http_logs {
      file_system {
        retention_in_days = 7
        retention_in_mb   = 35
      }
    }
  }

  tags = local.default_tags
}

# ---------------------------------------------------------------------------
# Application Insights
# ---------------------------------------------------------------------------

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${local.name_prefix}-logs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.default_tags
}

resource "azurerm_application_insights" "main" {
  name                = "${local.name_prefix}-insights"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "web"
  tags                = local.default_tags
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "postgres_fqdn" {
  value     = azurerm_postgresql_flexible_server.main.fqdn
  sensitive = true
}

output "storage_account_name" {
  value = azurerm_storage_account.data_lake.name
}

output "app_service_url" {
  value = "https://${azurerm_linux_web_app.api.default_hostname}"
}

output "app_insights_instrumentation_key" {
  value     = azurerm_application_insights.main.instrumentation_key
  sensitive = true
}

output "log_analytics_workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}
