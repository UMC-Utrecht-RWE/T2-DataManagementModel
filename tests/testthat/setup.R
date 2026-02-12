library(data.table)
library(DBI)
library(duckdb)
library(T2.DMM)

# ====================
# 1. SHARED METADATA
# ====================

shared_metadata <- data.table::data.table(
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
# 2. TEST CONFIGURATION FILE FOR set_database
# ====================
set_database <- '{
  "data_model": "ConcePTION",
  "file_format": "csv",
  "operations": {
    "DuplicateRemover": false,
    "MissingRemover": true,
    "UniqueIdGenerator": false,
    "ReportGenerator":false
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
  "missing_remover": {
    "columns": {
      "PERSONS": ["country_of_birth"],
      "VACCINES": ["vx_lot_num"]
    },
    "to_view": false
  }
}'

config_set_database <- file.path(tempdir(), "set_database.json")
writeLines(set_database, config_set_database)
Sys.setenv(CONFIG_SET_DB = config_set_database)

# ====================
# 3. TEST CONFIGURATION FILE
# ====================
config_json <- '{
  "data_model": "ConcePTION",
  "file_format": "csv",
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
  "duplicate_remover": {
    "save_path": "intermediate_data_file",
    "add_postfix": null,
    "save_deleted": true,
    "cdm_tables_columns": {
      "PERSONS": ["person_id", "country_of_birth"],
      "VACCINES": ["person_id", "vx_manufacturer"]
    },
    "to_view": false
  },
  "missing_remover": {
    "columns": {
      "PERSONS": ["country_of_birth"],
      "VACCINES": ["vx_lot_num"]
    },
    "to_view": false
  },
    "unique_id_generator":{
      "instance_name": "",
      "to_view": false
    },
    "report_generator": {
      "report_path": ".",
      "report_name": "count_rows_origin.fst"
    }
}'

config_path <- file.path(tempdir(), "config_path.json")
writeLines(config_json, config_path)
Sys.setenv(CONFIG_PATH = config_path)


# ====================
# 4. TEST CONFIGURATION FILE FOR set_database
# ====================
set_absent <- '{
  "data_model": "ConcePTION",
  "file_format": "csv",
  "operations": {
    "AbsentOperation": true
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
  ]
}'

config_set_absent <- file.path(tempdir(), "set_absent.json")
writeLines(set_absent, config_set_absent)
Sys.setenv(CONFIG_ABSENT = config_set_absent)

# ====================
# 5. APPLY_CODELIST TEST CONFIGURATION FILE
# ====================
config_json <- '{
  "data_model": "ConcePTION",
  "file_format": "csv",
  "operations": {
    "DuplicateRemover": true,
    "MissingRemover": true,
    "UniqueIdGenerator": true,
    "ReportGenerator": false
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
  "duplicate_remover": {
    "save_path": "intermediate_data_file",
    "add_postfix": null,
    "save_deleted": true,
    "cdm_tables_columns": {
      "PERSONS": ["person_id", "country_of_birth"],
      "VACCINES": ["person_id", "vx_manufacturer"]
    },
    "to_view": false
  },
  "missing_remover": {
    "columns": {
      "PERSONS": ["country_of_birth"],
      "VACCINES": ["vx_lot_num"]
    },
    "to_view": false
  },
    "unique_id_generator":{
      "instance_name": "",
      "to_view": false
    },
    "report_generator": {
      "report_path": ".",
      "report_name": "count_rows_origin.fst"
    }
}'

config_path <- file.path(tempdir(), "config_path.json")
writeLines(config_json, config_path)
Sys.setenv(APPLYCODELIST_CONFIG_PATH = config_path)