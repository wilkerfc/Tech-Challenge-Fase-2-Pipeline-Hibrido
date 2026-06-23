# =============================================================================
# Terraform – Infraestrutura GCP para Pipeline de Alfabetização
# FinOps: recursos dimensionados para minimizar custo
# =============================================================================

terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "tc-alfabetizacao-tfstate"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# Variáveis
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Ambiente (dev / prod)"
  type        = string
  default     = "dev"
}

variable "alert_email" {
  description = "E-mail para alertas de monitoramento"
  type        = string
}

# -----------------------------------------------------------------------------
# APIs
# -----------------------------------------------------------------------------

locals {
  required_apis = [
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudbuild.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.required_apis)
  service            = each.value
  disable_on_destroy = false
}

# -----------------------------------------------------------------------------
# Service Account
# -----------------------------------------------------------------------------

resource "google_service_account" "pipeline_sa" {
  account_id   = "alfabetizacao-pipeline"
  display_name = "Pipeline Alfabetização SA"
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/pubsub.editor",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# -----------------------------------------------------------------------------
# GCS – Data Lake (Medalhão)
# FinOps: lifecycle rules movem dados para Nearline/Coldline automaticamente
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "datalake" {
  name                        = "${var.project_id}-datalake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = var.environment == "dev"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["bronze/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age            = 365
      matches_prefix = ["bronze/"]
    }
    action {
      type = "Delete"
    }
  }

  # Silver: mantém 180 dias
  lifecycle_rule {
    condition {
      age            = 180
      matches_prefix = ["silver/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    project     = "alfabetizacao"
  }
}

# TF State bucket
resource "google_storage_bucket" "tfstate" {
  name                        = "${var.project_id}-tfstate"
  location                    = var.region
  uniform_bucket_level_access = true
  versioning { enabled = true }
}

# -----------------------------------------------------------------------------
# BigQuery – Gold Layer
# FinOps: particionamento por data reduz custo de queries
# -----------------------------------------------------------------------------

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = "gold_alfabetizacao"
  friendly_name               = "Gold – Alfabetização Analytics"
  location                    = var.region
  delete_contents_on_destroy  = var.environment == "dev"

  labels = {
    environment = var.environment
    layer       = "gold"
  }
}

resource "google_bigquery_table" "indicador_municipio" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "indicador_municipio"

  time_partitioning {
    type  = "DAY"
    field = "_gold_date"
  }

  clustering = ["sigla_uf", "ano"]

  schema = jsonencode([
    { name = "id_municipio",             type = "STRING",  mode = "REQUIRED" },
    { name = "nome",                     type = "STRING",  mode = "NULLABLE" },
    { name = "sigla_uf",                 type = "STRING",  mode = "NULLABLE" },
    { name = "nome_uf",                  type = "STRING",  mode = "NULLABLE" },
    { name = "ano",                      type = "INTEGER", mode = "REQUIRED" },
    { name = "indicador_alfabetizacao",  type = "FLOAT",   mode = "NULLABLE" },
    { name = "quantidade_matriculas",    type = "INTEGER", mode = "NULLABLE" },
    { name = "meta_municipio",           type = "FLOAT",   mode = "NULLABLE" },
    { name = "meta_nacional",            type = "FLOAT",   mode = "NULLABLE" },
    { name = "gap_vs_meta_municipio",    type = "FLOAT",   mode = "NULLABLE" },
    { name = "gap_vs_meta_nacional",     type = "FLOAT",   mode = "NULLABLE" },
    { name = "status_meta_municipio",    type = "STRING",  mode = "NULLABLE" },
    { name = "meta_atingida",            type = "BOOLEAN", mode = "NULLABLE" },
    { name = "_gold_date",               type = "DATE",    mode = "NULLABLE" },
    { name = "_gold_timestamp",          type = "STRING",  mode = "NULLABLE" },
  ])

  deletion_protection = false
}

resource "google_bigquery_table" "painel_nacional" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "painel_nacional"

  schema = jsonencode([
    { name = "ano",                          type = "INTEGER", mode = "REQUIRED" },
    { name = "indicador_medio_nacional",     type = "FLOAT",   mode = "NULLABLE" },
    { name = "total_municipios",             type = "INTEGER", mode = "NULLABLE" },
    { name = "municipios_meta_atingida",     type = "INTEGER", mode = "NULLABLE" },
    { name = "total_matriculas",             type = "INTEGER", mode = "NULLABLE" },
    { name = "meta_nacional",                type = "FLOAT",   mode = "NULLABLE" },
    { name = "pct_municipios_alfabetizados", type = "FLOAT",   mode = "NULLABLE" },
    { name = "gap_meta",                     type = "FLOAT",   mode = "NULLABLE" },
    { name = "_gold_timestamp",              type = "STRING",  mode = "NULLABLE" },
  ])

  deletion_protection = false
}

# -----------------------------------------------------------------------------
# Pub/Sub – Streaming
# -----------------------------------------------------------------------------

resource "google_pubsub_topic" "events" {
  name = "alfabetizacao-events"
  labels = { environment = var.environment }

  # Retenção de 7 dias (FinOps: não guardar mais que necessário)
  message_retention_duration = "604800s"
}

resource "google_pubsub_subscription" "events_sub" {
  name  = "alfabetizacao-events-sub"
  topic = google_pubsub_topic.events.name

  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"  # 1 dia
  retain_acked_messages      = false

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }

  labels = { environment = var.environment }
}

resource "google_pubsub_topic" "dlq" {
  name = "alfabetizacao-events-dlq"
}

# -----------------------------------------------------------------------------
# Cloud Scheduler – Batch Jobs (FinOps: execução periódica, não contínua)
# -----------------------------------------------------------------------------

resource "google_cloud_scheduler_job" "bronze_batch" {
  name             = "bronze-batch-daily"
  description      = "Dispara ingestão batch diária (Bronze)"
  schedule         = "0 2 * * *"  # 02:00 UTC diariamente
  time_zone        = "America/Sao_Paulo"
  attempt_deadline = "3600s"

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/run-bronze-batch"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "silver_batch" {
  name             = "silver-transform-daily"
  description      = "Dispara transformações Silver"
  schedule         = "0 4 * * *"  # 04:00 UTC
  time_zone        = "America/Sao_Paulo"
  attempt_deadline = "3600s"

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/run-silver"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "gold_batch" {
  name             = "gold-analytics-daily"
  description      = "Constrói camada Gold analítica"
  schedule         = "0 6 * * *"  # 06:00 UTC
  time_zone        = "America/Sao_Paulo"
  attempt_deadline = "3600s"

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/run-gold"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }
}

resource "google_cloud_scheduler_job" "quality_check" {
  name      = "quality-check-daily"
  schedule  = "0 7 * * *"
  time_zone = "America/Sao_Paulo"

  http_target {
    uri         = "https://${var.region}-${var.project_id}.cloudfunctions.net/run-quality"
    http_method = "POST"
    oidc_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }
}

# -----------------------------------------------------------------------------
# Monitoring – Alertas
# -----------------------------------------------------------------------------

resource "google_monitoring_notification_channel" "email" {
  display_name = "Pipeline Alerts Email"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
}

resource "google_monitoring_alert_policy" "pipeline_errors" {
  display_name = "Pipeline Errors Alert"
  combiner     = "OR"

  conditions {
    display_name = "Pipeline error rate elevada"
    condition_threshold {
      filter          = "metric.type=\"${METRIC_PREFIX}/pipeline_errors_total\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 5
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
  alert_strategy {
    auto_close = "86400s"
  }
}

resource "google_monitoring_alert_policy" "quality_degradation" {
  display_name = "Qualidade de Dados Degradada"
  combiner     = "OR"

  conditions {
    display_name = "Pass rate abaixo de 80%"
    condition_threshold {
      filter          = "metric.type=\"${METRIC_PREFIX}/quality_pass_rate\""
      duration        = "300s"
      comparison      = "COMPARISON_LT"
      threshold_value = 80
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email.name]
}

locals {
  METRIC_PREFIX = "custom.googleapis.com/alfabetizacao_pipeline"
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "datalake_bucket" {
  value = google_storage_bucket.datalake.name
}

output "bq_gold_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "pubsub_topic" {
  value = google_pubsub_topic.events.name
}

output "pipeline_sa_email" {
  value = google_service_account.pipeline_sa.email
}
