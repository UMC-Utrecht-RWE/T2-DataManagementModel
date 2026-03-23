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
reference_codelist <- data.table::fread(file.path(getwd(),"data/code_list.csv"))

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

## -----------------------------------------------------------------------------
# Transform to long format the codelist to comply with the apply_codelist() function
dap_specific_codelist <- dap_specific_codelist[match_status == "MATCHED"]
dap_specific_codelist[, keep_value_column_name := NA_character_ ]
dap_specific_codelist[, keep_date_column_name := "start_date_record" ]

long_format_codelist <- wrangling_codelist(
    codelist = dap_specific_codelist,
    code_column_name = "event_code",
    codingsystem_column_name = "event_record_vocabulary"
  )

# View results
DT::datatable(long_format_codelist,options = list(scrollX = TRUE))

## -----------------------------------------------------------------------------
# Load your concept map in RWE BRIDGE metadata format
# Expected columns: concept_id, cdm_table_name, 
#                   column_name_*, expected_value_*,
#                   keep_value_column_name, keep_date_column_name

dap_specific_concept_map <- data.table::fread(file.path(getwd(),"data/dap_specific_concept_map.csv"))

# Transform to long format
long_format_concept_map <- wrangling_concept_map(dap_specific_concept_map)


## -----------------------------------------------------------------------------
DT::datatable(dap_specific_concept_map,options = list(scrollX = TRUE))

## -----------------------------------------------------------------------------
DT::datatable(long_format_concept_map,options = list(scrollX = TRUE))

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

## -----------------------------------------------------------------------------
long_format_codelist[, id_set := paste0("codelist-",id_set)]
#Selecting the concepts for the ConcePTION CDM since our databse follows that CDM
long_format_concept_map <- long_format_concept_map[toupper(cdm_name)
                                                          %in% "CONCEPTION"]
long_format_concept_map[, id_set := paste0("conceptmap-",id_set)]
codelist <- rbindlist(list(long_format_codelist,long_format_concept_map), 
                      use.names = TRUE, fill = TRUE)
# Apply the prepared codelist to the database
apply_codelist(
  db_con = db_connection,
  codelist = codelist,
  materialize = "in_database",  # or "in_parquet"
  path_parquets = NULL,  # "/path/to/save" if materialize = "in_parquet"
  keep_id_set = TRUE
)

## -----------------------------------------------------------------------------
concept_table <- dbReadTable(db_connection,"concept_table")
DT::datatable(concept_table,options = list(scrollX = TRUE))

## -----------------------------------------------------------------------------
sessionInfo()

