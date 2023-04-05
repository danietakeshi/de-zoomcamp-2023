locals {
  data_lake_bucket = "zoomcamp_bucket_project"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "southamerica-east1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "dezoomcamp_project"
}

variable "fixture_table" {
  description = "BigQuery Table that fixtures raw data (from GCS) will be written to"
  type = string
  default = "ods_football_fixtures"
}

variable "stats_table" {
  description = "BigQuery Table that Statistics raw data (from GCS) will be written to"
  type = string
  default = "ods_football_stats"
}
