provider "google" {
  alias   = "enablement"
  project = var.project_id
  region  = var.region
}

# 0️⃣ Bootstrap Cloud Resource Manager so Terraform can even call Service Usage
resource "null_resource" "enable_crm_api" {
  provisioner "local-exec" {
    command = "gcloud services enable cloudresourcemanager.googleapis.com --project=${var.project_id}"
  }
}

# 1️⃣ Service Usage
resource "google_project_service" "service_usage" {
  provider   = google.enablement
  project    = var.project_id
  service    = "serviceusage.googleapis.com"
  depends_on = [null_resource.enable_crm_api]
}

# 2️⃣ Cloud Resource Manager
resource "google_project_service" "cloud_resource_manager" {
  provider   = google.enablement
  project    = var.project_id
  service    = "cloudresourcemanager.googleapis.com"
  depends_on = [google_project_service.service_usage]
}

# 3️⃣ IAM API
resource "google_project_service" "iam" {
  provider   = google.enablement
  project    = var.project_id
  service    = "iam.googleapis.com"
  depends_on = [google_project_service.cloud_resource_manager]
}

# 4️⃣ Cloud SQL Admin
resource "google_project_service" "cloud_sql" {
  provider   = google.enablement
  project    = var.project_id
  service    = "sqladmin.googleapis.com"
  depends_on = [google_project_service.iam]
}

# 5️⃣ AI Platform (optional)
resource "google_project_service" "ai_platform" {
  provider   = google.enablement
  project    = var.project_id
  service    = "aiplatform.googleapis.com"
  depends_on = [google_project_service.cloud_sql]
}