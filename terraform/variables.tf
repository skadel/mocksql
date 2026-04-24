variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "The Google Cloud region"
  type        = string
  default     = "europe-west1"
}

variable "executor_account_email" {
  description = "Email of the account running Terraform"
  type        = string
}

variable "db_instance_name" {
  description = "The name of the Cloud SQL instance"
  type        = string
  default     = "mocksql"
}

variable "test_data_id" {
  description = "The bigquery dataset used for bigquery compilation"
  type        = string
  default     = "test_dataset"
}

variable "postgres_admin_password" {
  description = "Mot de passe admin Postgres"
  type        = string
  sensitive   = true
}
