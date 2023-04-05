terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}

resource "google_bigquery_table" "table1" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.fixture_table
  deletion_protection = false

  time_partitioning {
    field = "fixture_date"
    type = "DAY"
  }

  clustering = ["league_name"]

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "fixture_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_referee",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_timezone",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_date",
    "type": "TIMESTAMP"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_timestamp",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_periods_first",
    "type": "FLOAT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_periods_second",
    "type": "FLOAT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_venue_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_venue_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_venue_city",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_status_long",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_status_short",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "fixture_status_elapsed",
    "type": "FLOAT64"
  },
  {
    "mode": "NULLABLE",
    "name": "league_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "league_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "league_country",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "league_logo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "league_flag",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "league_season",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "league_round",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_home_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_home_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_home_logo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_home_winner",
    "type": "BOOL"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_away_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_away_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_away_logo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "teams_away_winner",
    "type": "BOOL"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_home",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_away",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_halftime_home",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_halftime_away",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_fulltime_home",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_fulltime_away",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_extratime_home",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_extratime_away",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_penalty_home",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "score_penalty_away",
    "type": "INT64"
  }
]
EOF

}


resource "google_bigquery_table" "table2" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = var.stats_table
  deletion_protection = false

  clustering = ["fixture_id"]

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "fixture_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "team_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "team_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "team_logo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "player_id",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "player_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "player_photo",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "offsides",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "games_minutes",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "games_number",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "games_position",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "games_rating",
    "type": "FLOAT64"
  },
  {
    "mode": "NULLABLE",
    "name": "games_captain",
    "type": "BOOL"
  },
  {
    "mode": "NULLABLE",
    "name": "games_substitute",
    "type": "BOOL"
  },
  {
    "mode": "NULLABLE",
    "name": "shots_total",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "shots_on",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_total",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_conceded",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_assists",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "goals_saves",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "passes_total",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "passes_key",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "passes_accuracy",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "tackles_total",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "tackles_blocks",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "tackles_interceptions",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "duels_total",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "duels_won",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "dribbles_attempts",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "dribbles_success",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "dribbles_past",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fouls_drawn",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "fouls_committed",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "cards_yellow",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "cards_red",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "penalty_won",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "penalty_commited",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "penalty_scored",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "penalty_missed",
    "type": "INT64"
  },
  {
    "mode": "NULLABLE",
    "name": "penalty_saved",
    "type": "INT64"
  }
]
EOF

}