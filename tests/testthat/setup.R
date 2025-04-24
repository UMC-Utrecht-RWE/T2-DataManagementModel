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
# 2. TEMP DATABASE
# ====================

# Define a persistent database path
db_path <- tempfile(fileext = ".duckdb")

# Connect to DuckDB with file backing
conn <- dbConnect(duckdb::duckdb(), db_path)

# Define table schemas
table_definitions <- list(
  EVENTS = "id TEXT, event_name TEXT, event_date DATE",
  MEDICAL_OBSERVATIONS = "id TEXT, observation TEXT, patient_id INTEGER",
  MEDICINES = "id TEXT, medicine_name TEXT, dosage TEXT",
  OBSERVATION_PERIODS = "id TEXT, start_date DATE, end_date DATE",
  PERSONS = "id INTEGER, name TEXT, birth_date DATE",
  SURVEY_ID = "id TEXT, survey_name TEXT",
  SURVEY_OBSERVATIONS = "id TEXT, survey_id INTEGER, observation TEXT",
  VACCINES = "id TEXT, vaccine_name TEXT, manufacturer TEXT",
  VISIT_OCCURRENCE = "id TEXT, visit_date DATE, patient_id INTEGER"
)

# Create tables
lapply(names(table_definitions), function(tbl) {
  dbExecute(
    conn,
    sprintf("CREATE TABLE %s (%s);", tbl, table_definitions[[tbl]])
  )
})

# Insert minimal dummy records
insert_statements <- list(
  "INSERT INTO EVENTS VALUES (1, 'Event A', '2025-01-01')",
  "INSERT INTO MEDICAL_OBSERVATIONS VALUES (1, 'Observation A', 1)",
  "INSERT INTO MEDICINES VALUES (1, 'Medicine A', '10mg')",
  "INSERT INTO OBSERVATION_PERIODS VALUES (1, '2025-01-01', '2025-12-31')",
  "INSERT INTO PERSONS VALUES (1, 'John Doe', '1990-01-01'),",
  "INSERT INTO SURVEY_ID VALUES (1, 'Survey A')",
  "INSERT INTO SURVEY_OBSERVATIONS VALUES (1, 1, 'Survey Observation A')",
  "INSERT INTO VACCINES VALUES (1, 'Vaccine A', 'Manufacturer A')",
  "INSERT INTO VISIT_OCCURRENCE VALUES (1, '2025-04-22', 1)"
)

lapply(insert_statements, dbExecute, conn = conn)

# Assign connection and DB path to global environment / env vars
assign("SYNTHETIC_DB_CONN", conn, envir = .GlobalEnv)
Sys.setenv(SYNTHETIC_DB_PATH = db_path)

# ====================
# 3. TEST CONFIGURATION FILE
# ====================
config_json <- '{
  "data_model": "ConcePTION",
  "operations_path": "R/scripts/operations",
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
