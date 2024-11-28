# Author: Albert Cid Royo
# email: a.cidroyo@umcutrecht.com & Y.Mao@umcutrecht.nl
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 05/12/2022

# This script uses the different functions from the package
# The aim of the script is to:
# Load all the tables of interest from the CDM into a SQLite Database
# by:
# 1. Connecting to a new Database ori (origin)
# 2. Reading the cdm_metadata file
# 3. Loading all CSV files form the D2 folder to the d2.db
# 4. Option: Exclude duplicate rows
# 5. Create unique id per row

print(paste0("[Set Data Base]: Starting import into Data Base "))

#################
# Loading files into the origin database
################

filepath_cdm_metadata <-
  file.path("data", "concePTION_metadata_v2.rda")
load(filepath_cdm_metadata, envir = 'cdm_metadata')
cdm_metadata <- concePTION_metadata_v2

path_to_db = 'FILL_THIS_UP' #path of the new database
d2_db_conn <- duckdb::dbConnect(duckdb::duckdb(), path_to_db)

cdm_tables_names = c("PERSONS", "VACCINES", "MEDICINES") #Add as many table from the CDM as you wish

dir_data = 'FILL_THIS_UP' #path to the data instance

load_db(
  db_connection = d2_db_conn, csv_path_dir = dir_data,
  cdm_metadata = cdm_metadata, cdm_tables_names = cdm_tables_names
)

#################
# OPTIONAL: Deleting duplicates
################

delete_duplicates_flag <- TRUE
if (delete_duplicates_flag == TRUE) {
  # The scheme defines the columns names (* = all) used for identifying unique records. The scheme is defined for every table in the CDM
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = d2_db_conn, scheme, save_deleted = FALSE
  )
  rm(scheme)
}

#################
# Generate Unique ID of each record.
################

mandatory_columns <- cdm_metadata[Mandatory == 'Yes']
order_by_cols <- lapply(split(mandatory_columns$Variable, mandatory_columns$TABLE), function(x) x) 
create_unique_id(d2_db_conn, cdm_tables_names, extension_name = "", order_by_cols = order_by_cols)

rm(cdm_metadata,order_by_cols)
#################
# OPTIONAL: Deleting rows with missing values for specific columns
################

list_colums_clean = list(PERSONS = c('person_id'), VACCINES = c('person_id'), OBSERVATION_PERIODS = c('person_id'),
                         MEDICAL_OBSERVATIONS = c('person_id'), MEDICINES = c('person_id'), EVENTS = c('person_id'), 
                         SURVEY_OBSERVATIONS = c('person_id'), SURVEY_ID = c('person_id'), VISIT_OCCURRENCE = c('person_id'))

delete_missings_flag <- FALSE
if (delete_missings_flag == TRUE) {
  log_print("[Deleting rows with missing values]")
  for(index in 1:length(list_colums_clean)){
    table_to_clean <- names(list_colums_clean[index])
    log_print(paste0("[Deleting rows with missing values] ", table_to_clean))
    lapply(list_colums_clean[index], function(x) {
      rs <- dbSendStatement(d2_db_conn,paste0("DELETE FROM ",table_to_clean,
                                              " WHERE ",x," IS NULL OR ",x," = '' "))
      num_rows <- duckdb::dbGetRowsAffected(rs)
      print(paste0("Number of record deleted: ", num_rows))
      duckdb::dbClearResult(rs)
    })
    rm(table_to_clean)
  }
}

#################
# OPTIONAL: Report the number of rows per table, this is useful for early detection of missing records
################

if (count_rows_flag == FALSE) {
  count_rows_origin <- get_rows_tables(d2_db_conn)
  print("[GetRowsTablesDataBase] Origin Database values")
  print(count_rows_origin)
  
  dir_save_count_row <- file.path(
    dir_intermediate_t2,
    'count_rows_origin.fst')
  write_fst(count_rows_origin, dir_save_count_row)
  rm(count_rows_origin)
}


duckdb::dbDisconnect(d2_db_conn, shutdown = TRUE)
rm(d2_db_conn,mandatory_columns,list_colums_clean, index)
gc()
print(paste0("[Set Data Base]: IMPORT ACCOMPLISHED"))
