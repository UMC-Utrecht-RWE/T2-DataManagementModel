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
dap_codes <- get_unique_codelist(
  db_connection = db_connection,
  column_info_list = column_info_list,
  tb_name = "EVENTS"
)

dap_codes <- as.data.table(dap_codes[[1]])
# View the results
DT::datatable(dap_codes,options = list(scrollX = TRUE))

## -----------------------------------------------------------------------------
# Load your reference codelist
# This typically contains all standard codes and their mappings
reference_codelist <- data.table::fread("data/code_list.csv")

# Create a standardized codelist with intelligent matching
dap_specific_codelist <- create_dap_specific_codelist(
  dap_codes = dap_codes,
  codelist = reference_codelist,
  start_with_codingsystems = c(
    "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
    "ICPC", "ICPC2P", "ICPC2EENG", "ATC"
  )
)

# View results
DT::datatable(dap_specific_codelist,options = list(scrollX = TRUE))

