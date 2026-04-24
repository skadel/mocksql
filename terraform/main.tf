provider "google" {
  alias   = "default"
  project = var.project_id
  region  = var.region
}

# Create a Service Account for SQL Chat
resource "google_service_account" "mocksql" {
  provider     = google.default
  account_id   = "mocksql"
  display_name = "Service Account for SQL Chat"
  depends_on   = [
    google_project_service.cloud_sql
  ]
}

# IAM Permissions for Executor Account
resource "google_project_iam_member" "executor_service_account_admin" {
  provider = google.default
  project  = var.project_id
  role     = "roles/iam.serviceAccountAdmin"
  member   = "user:${var.executor_account_email}"
  depends_on = [
    google_project_service.cloud_resource_manager
  ]
}

resource "google_project_iam_member" "executor_cloudsql_admin" {
  provider = google.default
  project  = var.project_id
  role     = "roles/cloudsql.admin"
  member  = "serviceAccount:${google_service_account.mocksql.email}"
  depends_on = [
    google_project_service.cloud_resource_manager
  ]
}


resource "google_project_iam_member" "executor_cloudsql_client" {
  provider = google.default
  project  = var.project_id             # e.g. "mocksql-457313"
  role     = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.mocksql.email}"
  depends_on = [
    google_project_service.cloud_sql    # ensure API is enabled first
  ]
}

# Assign Roles to the Service Account
resource "google_project_iam_member" "sqlchat_roles" {
  provider = google.default
  for_each = toset([
    "roles/cloudsql.client",
    "roles/cloudsql.instanceUser",
    "roles/aiplatform.user",
    "roles/bigquery.dataViewer",
    "roles/bigquery.user",
    "roles/iam.serviceAccountUser"
  ])
  project = var.project_id
  member  = "serviceAccount:${google_service_account.mocksql.email}"
  role    = each.key
  depends_on = [
    google_service_account.mocksql
  ]
}

# Create a Cloud SQL Database Instance
resource "google_sql_database_instance" "postgres_instance" {
  provider            = google.default
  name                = var.db_instance_name
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = true

  settings {
    tier = "db-g1-small"
    edition  = "ENTERPRISE"

    ip_configuration {
      ipv4_enabled = true
      authorized_networks {
        name  = "all-networks"
        value = "0.0.0.0/0"
      }
    }

    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }
  }

  depends_on = [
    google_project_service.cloud_sql
  ]
}

resource "google_service_account_iam_member" "allow_token_creation" {
  provider  = google.default
  # points at the mocksql SA you already created
  service_account_id = google_service_account.mocksql.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.executor_account_email}"
  # make sure this waits until the SA itself exists:
  depends_on         = [ google_service_account.mocksql ]
}


# Create Databases
resource "google_sql_database" "default" {
  provider        = google.default
  name            = "mocksqldb"
  instance        = google_sql_database_instance.postgres_instance.name
  project         = var.project_id
  deletion_policy = "DELETE"
  depends_on      = [
    google_sql_database_instance.postgres_instance
  ]
}

resource "google_sql_database" "sqlmesh" {
  provider        = google.default
  name            = "sqlmeshconf"
  instance        = google_sql_database_instance.postgres_instance.name
  project         = var.project_id
  deletion_policy = "DELETE"
  depends_on      = [
    google_sql_database.default
  ]
}

# Create an IAM User for the Service Account
resource "google_sql_user" "iam_service_account_user" {
  name     = trimsuffix(google_service_account.mocksql.email, ".gserviceaccount.com")
  instance = google_sql_database_instance.postgres_instance.name
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
  project  = var.project_id
  depends_on = [
    google_sql_database_instance.postgres_instance
  ]
}

# Assign Additional Roles to the Service Account
resource "google_project_iam_member" "sqlchat_additional_roles" {
  provider = google.default
  for_each = toset([
    "roles/cloudsql.admin",
    "roles/cloudsql.client",
    "roles/cloudsql.editor",
    "roles/bigquery.dataViewer",
    "roles/bigquery.user",
    "roles/cloudsql.instanceUser",
    "roles/iam.serviceAccountUser",
    "roles/aiplatform.user",
    "roles/secretmanager.secretAccessor"
  ])
  project = var.project_id
  member  = "serviceAccount:${google_service_account.mocksql.email}"
  role    = each.key
  depends_on = [
    google_sql_user.iam_service_account_user
  ]
}

resource "google_bigquery_dataset" "mocksql_dataset" {
  provider   = google.default
  project    = var.project_id
  dataset_id = var.test_data_id
  location   = var.region
}

resource "google_bigquery_dataset_iam_member" "mocksql_ds_writer" {
  provider   = google.default
  project    = var.project_id
  dataset_id = google_bigquery_dataset.mocksql_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.mocksql.email}"

  depends_on = [
    google_bigquery_dataset.mocksql_dataset,
    google_service_account.mocksql
  ]
}


# resource "null_resource" "grant_schema_permissions" {
#   triggers = {
#     instance = google_sql_database_instance.postgres_instance.name
#     user     = google_sql_user.iam_service_account_user.name
#   }
#
#   depends_on = [
#     google_project_iam_member.sqlchat_additional_roles,
#     google_sql_database_instance.postgres_instance
#   ]
#
#   provisioner "local-exec" {
#     interpreter = ["PowerShell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command"]
#
#     command = <<-EOFS
#       # 1. Prépare le script SQL (avec \q pour forcer la sortie de psql)
#       $sql = @"
#       GRANT USAGE, CREATE ON SCHEMA public TO "${google_sql_user.iam_service_account_user.name}";
#       GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "${google_sql_user.iam_service_account_user.name}";
#       ALTER DEFAULT PRIVILEGES IN SCHEMA public
#         GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "${google_sql_user.iam_service_account_user.name}";
#       CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
#       \q
#       "@
#       $sql | Out-File temp_sql_commands.sql -Encoding UTF8
#
#       # 2. Attends que l'instance soit RUNNABLE
#       Write-Host "→ Attente de l'état RUNNABLE..."
#       do {
#         Start-Sleep -Seconds 5
#         $state = gcloud sql instances describe ${google_sql_database_instance.postgres_instance.name} `
#                   --format="value(state)"
#         Write-Host "  État actuel :" $state
#       } while ($state -ne "RUNNABLE")
#
#       # 3. Attends que la dernière opération soit DONE
#       Write-Host "→ Attente de la fin des opérations Cloud SQL..."
#       do {
#         Start-Sleep -Seconds 5
#         $opStatus = gcloud sql operations list `
#                      --instance=${google_sql_database_instance.postgres_instance.name} `
#                      --sort-by=~startTime --limit=1 `
#                      --format="value(status)"
#         Write-Host "  Dernier statut d'opération :" $opStatus
#       } while ($opStatus -ne "DONE")
#
#       # 4. Tente la connexion et exécution du SQL
#       Write-Host "→ Application des grants..."
#       Get-Content temp_sql_commands.sql | `
#         & gcloud sql connect ${google_sql_database_instance.postgres_instance.name} `
#             --user=postgres --database=mocksqldb --quiet
#
#       Get-Content temp_sql_commands.sql | `
#         & gcloud sql connect ${google_sql_database_instance.postgres_instance.name} `
#             --user=postgres --database=sqlmeshconf --quiet
#
#       # 5. Ménage
#       Remove-Item temp_sql_commands.sql -Force
#       Write-Host "✅ Grants appliqués avec succès."
#     EOFS
#   }
# }
