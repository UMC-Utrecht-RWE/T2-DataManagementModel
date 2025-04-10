# Author: Albert Cid Royo
# email: a.cidroyo@umcutrecht.com & Y.Mao@umcutrecht.nl #nolint
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 05/12/2022 #nolint

# This script uses the different functions from the package
# The aim of the script is to:
# Load all the tables of interest from the CDM into a SQLite Database
# by:
# 1. Connecting to a new Database ori (origin)
# 2. Reading the cdm_metadata.rds file
# 3. Loading all CSV files form the D2 folder to the d2.db
# 4. Option: Exclude duplicate rows
# 5. Create unique id per row

print(paste0("[Set Data Base]: Starting import into Data Base "))

#################
# Loading files into the origin database
################

cdm_metadata <- as.data.table(
  readRDS(file.path(transformations_common_configuration, "CDM_metadata.rds"))
)

dir_d2_db <- file.path(
  transformations_T2_semantic_harmonization_intermediate_data_file,
  "d2.db"
)

if (file.exists(dir_d2_db)) {
  file.remove(dir_d2_db)
}
db_connection_origin <- dbConnect(RSQLite::SQLite(), dir_d2_db)

cdm_tables_names <- c(
  "PERSONS", "VACCINES", "OBSERVATION_PERIODS", "MEDICAL_OBSERVATIONS",
  "MEDICINES", "EVENTS", "SURVEY_OBSERVATIONS", "SURVEY_ID", "VISIT_OCCURRENCE"
)
load_db(
  db_connection = db_connection_origin, csv_path_dir = data_D2_cdm,
  cdm_metadata = cdm_metadata, cdm_tables_names = cdm_tables_names
)

#################
# OPTIONAL: Deleting duplicates
################

delete_duplicates_flag <- TRUE
if (delete_duplicates_flag == TRUE) {
  # The scheme defines the columns names (* = all) used for identifying unique
  #  records. The scheme is defined for every table in the CDM
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = db_connection_origin, scheme, save_deleted = TRUE,
    save_path = transformations_T2_semantic_harmonization_intermediate_data_file
  )
}

#################
# Generate Unique ID of each record.
################

create_unique_id(db_connection_origin, cdm_tables_names, extension_name = "")

#################
# OPTIONAL: Report the number of rows per table, this is useful for early
# detection of missing records
################

count_rows_flag <- TRUE

if (count_rows_flag == TRUE) {
  count_rows_origin <- get_rows_tables(db_connection_origin)
  print("[GetRowsTablesDataBase] Origin Database values")
  print(count_rows_origin)

  dir_save_count_row <- file.path(
    transformations_T2_semantic_harmonization_intermediate_data_file,
    "count_rows_origin.rds"
  )
  saveRDS(count_rows_origin, dir_save_count_row)
}


dbDisconnect(db_connection_origin)

print(paste0("[Set Data Base]: IMPORT ACCOMPLISHED"))
