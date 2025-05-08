library(data.table)
library(DBI)
library(duckdb)

# ====================
# 1. SHARED METADATA
# ====================

shared_metadata <- data.table(
  person_id = sprintf("#ID-%08d#", 1:5),
  Date = seq(18000, by = 123, length.out = 5),
  Voc = rep("ICD10", 5),
  Value = rep(1, 5),
  Outcome = c(
    "FAKE_COV_1", "FAKE_COV_2", "FAKE_COV_3",
    "FAKE_COV_1", "FAKE_COV_2"
  )
)

shared_metadata_path <- file.path(tempdir(), "shared_metadata.rds")
saveRDS(shared_metadata, shared_metadata_path)
Sys.setenv(SHARED_METADATA_PATH = shared_metadata_path)

# ====================
# 2. TEST CONFIGURATION FILE
# ====================
config_json <- '{
  "data_model": "ConcePTION",
  "operations": {
    "DuplicateRemover": true,
    "MissingRemover": true,
    "UniqueIdGenerator": true,
    "ReportGenerator": true
  },
  "cdm_tables_names": [
    "PERSONS",
    "VACCINES",
    "OBSERVATION_PERIODS",
    "MEDICAL_OBSERVATIONS",
    "MEDICINES",
    "EVENTS",
    "SURVEY_OBSERVATIONS",
    "SURVEY_ID",
    "VISIT_OCCURRENCE"
  ],
  "instance_name": "",
  "id_name" : "ori_id",
  "list_colums_clean": {
    "PERSONS": ["person_id"],
    "VACCINES": ["person_id"],
    "OBSERVATION_PERIODS": ["person_id"],
    "MEDICAL_OBSERVATIONS": ["person_id"],
    "MEDICINES": ["person_id"],
    "EVENTS": ["person_id"],
    "SURVEY_OBSERVATIONS": ["person_id"],
    "SURVEY_ID": ["person_id"],
    "VISIT_OCCURRENCE": ["person_id"]
  },
  "report": {
    "report_path": "data",
    "report_name": "count_rows_origin.fst"
  }
}'

config_path <- file.path(tempdir(), "set_db.json")
writeLines(config_json, config_path)
Sys.setenv(CONFIG_PATH = config_path)
