## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----eval=FALSE---------------------------------------------------------------
# # Install the package (example installation method)
# devtools::install_github("UMC-Utrecht-RWE/T2-DataManagementModel")

## -----------------------------------------------------------------------------
library(T2.DMM)
library(data.table)
library(DBI)
library(duckdb)

## -----------------------------------------------------------------------------
  db_path <- tempfile(pattern = "cdm", fileext = ".duckdb")

## -----------------------------------------------------------------------------
# Initialize the DatabaseLoader
loader <- DatabaseLoader$new(
  db_path = db_path,
  data_instance = file.path(getwd(),"data/"),
  config_path = file.path(getwd(),"data/set_db.json"),
  cdm_metadata = file.path(getwd(),"data/CDM_metadata.rds")
)

## -----------------------------------------------------------------------------
# Load the data into the database
loader$set_database()

## -----------------------------------------------------------------------------
# Execute all configured operations
loader$run_db_ops()

## -----------------------------------------------------------------------------
# Reconnect to database if needed
db_connection <- DBI::dbConnect(duckdb::duckdb(), db_path)

# Define which columns to extract as codelists
column_info_list <- list(
  list(
    source_column = c("event_code", "event_record_vocabulary"),
    alias_name = c("code", "coding_system")
  )
)

# Extract unique codelists
dap_codes <- T2.DMM::get_unique_codelist(
  db_connection = db_connection,
  column_info_list = column_info_list,
  tb_name = "EVENTS"
)

dap_codes <- as.data.table(dap_codes[[1]])
# View the results
DT::datatable(dap_codes)

## -----------------------------------------------------------------------------
# Load your reference codelist
# This typically contains all standard codes and their mappings
reference_codelist <- data.table::fread(file.path(getwd(),"data/code_list.csv"))

# Create a standardized codelist with intelligent matching
dap_specific_codelist <- T2.DMM::create_dap_specific_codelist(
  dap_codes = dap_codes,
  codelist = reference_codelist,
  start_with_codingsystems = c(
    "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
    "ICPC", "ICPC2P", "ICPC2EENG", "ATC"
  )
)

# View results
DT::datatable(dap_specific_codelist)

## -----------------------------------------------------------------------------
# Load your concept map in RWE BRIDGE metadata format
# Expected columns: concept_id, cdm_table_name, 
#                   column_name_*, expected_value_*,
#                   keep_value_column_name, keep_date_column_name

dap_specific_concept_map <- data.table::fread(file.path(getwd(),"data/dap_specific_concept_map.csv"))

# Transform to long format
long_format_codelist <- T2.DMM::wrangling_concept_map(dap_specific_concept_map)


## -----------------------------------------------------------------------------
DT::datatable(dap_specific_concept_map,options = list(scrollX = TRUE))

## -----------------------------------------------------------------------------
DT::datatable(long_format_codelist,options = list(scrollX = TRUE))

## ----eval=FALSE---------------------------------------------------------------
# # Initialize the concept table as a database table
# initialize_concept_table(
#   con = db_connection,
#   type_table = "table",  # or "view" for Parquet-backed table
#   partition = TRUE,
#   add_id_set = TRUE
# )
# 
# # Alternatively, initialize as a view reading from Parquet files
# initialize_concept_table(
#   con = db_connection,
#   type_table = "view",
#   path_parquets = "/path/to/parquet/files",
#   partition = TRUE,
#   add_id_set = TRUE
# )

## ----eval=FALSE---------------------------------------------------------------
# # Apply the prepared codelist to the database
# apply_codelist(
#   db_con = db_connection,
#   codelist = long_format_codelist,
#   materialize = "in_database",  # or "in_parquet"
#   path_parquets = NULL,  # "/path/to/save" if materialize = "in_parquet"
#   keep_id_set = TRUE
# )

## ----eval=FALSE---------------------------------------------------------------
# # ============================================================================
# # PROCESS 1: SET DATABASE
# # ============================================================================
# 
# # 1.1 Initialize loader
# loader <- DatabaseLoader$new(
#   db_path = "analysis.duckdb",
#   data_instance = "./data/cdm_instance",
#   config_path = "./config/loader_config.json",
#   cdm_metadata = "./data/CDM_metadata.rds"
# )
# 
# # 1.2 Load CSV/Parquet files
# loader$set_database()
# 
# # 1.3 Execute transformations (remove missing, add IDs, remove duplicates, report)
# loader$run_db_ops()
# 
# # ============================================================================
# # PROCESS 2: PROCESS CODELIST
# # ============================================================================
# 
# db_connection <- DBI::dbConnect(duckdb::duckdb(), "analysis.duckdb")
# 
# # 2.1 Get unique codelists
# column_info <- list(
#   list(source_column = "event_code", alias_name = "code")
# )
# unique_codelists <- get_unique_codelist(
#   db_connection,
#   column_info,
#   "EVENTS"
# )
# 
# # 2.2 Create DAP-specific codelist
# reference_codes <- data.table::fread("reference_codes.csv")
# dap_specific <- create_dap_specific_codelist(
#   dap_codes = unique_codelists[[1]],
#   codelist = reference_codes
# )
# 
# # ============================================================================
# # PROCESS 3: APPLY CODELIST
# # ============================================================================
# 
# # 3.1 Prepare concept map
# concept_map <- data.table::fread("concept_map.csv")
# long_codelist <- wrangling_concept_map(concept_map)
# 
# # 3.2 Initialize concept table
# initialize_concept_table(
#   db_connection,
#   type_table = "table",
#   add_id_set = TRUE
# )
# 
# # 3.3 Apply the codelist
# apply_codelist(
#   db_con = db_connection,
#   codelist = long_codelist,
#   materialize = "in_database",
#   keep_id_set = TRUE
# )
# 
# # Verify harmonized concepts
# harmonized <- DBI::dbReadTable(db_connection, "concept_table")
# head(harmonized)
# 
# # Clean up
# DBI::dbDisconnect(db_connection)

## -----------------------------------------------------------------------------
sessionInfo()

