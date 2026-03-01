# variables.tf
# Define todas las variables del proyecto

variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "ecommerce-olist-150226" 
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "southamerica-west1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "southamerica-west1-a"
}

variable "environment" {
  description = "Environment (dev/staging/prod)"
  type        = string
  default     = "dev"
}

# IMPORTANTE: misma región que el proyecto
variable "bucket_location" {
  description = "GCS Bucket Location"
  type        = string
  default     = "southamerica-west1"
}

variable "bigquery_location" {
  description = "BigQuery Dataset Location"
  type        = string
  default     = "southamerica-west1"
}

variable "labels" {
  description = "Common labels for all resources"
  type        = map(string)
  default = {
    project     = "olist-data-pipeline"
    environment = "dev"
    managed_by  = "terraform"
    region      = "cl"
  }
}
