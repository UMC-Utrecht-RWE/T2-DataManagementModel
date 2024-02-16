# Author: Albert Cid Royo
# email: a.cidroyo@umcutrecht.com
# Organisation: UMC Utrecht, Utrecht, The Netherlands
# Date: 05/12/2022

#This script uses the different funcitons from the package
# The aim of the script is to:
# Load all the tables of interest from the CDM into a SQLite Database
# by:
# 1. Connecting to a new Database ori
# 2. Reading the cdm_metadata.rds file
# 3. Loading all CSV files form the D2 folder to the d2.db
# 4. Option: Exclude duplicate rows
# 5. Create unique id per row
# 6. 
print(paste0('[Set Data Base]: Starting import into Data Base '))

# Packages needed
if (!require("DBI")) {
  install.packages("DBI")
}
suppressPackageStartupMessages(library(DBI))
if (!require("RSQLite")) {
  install.packages("RSQLite")
}
suppressPackageStartupMessages(library(RSQLite))
if (!require(data.table)) {
  install.packages("data.table")
}
suppressPackageStartupMessages(library(data.table))

if (!require(stringr)) {
  install.packages("stringr")
}
suppressPackageStartupMessages(library(stringr))

#################
#Loading files into the origin database
################

transformations_T2_semantic_harmonization_configuration <- "/Users/ymao/Library/CloudStorage/OneDrive-UMCUtrecht/Work_UMC_230726/Paxlovid/Paxlovid-1047/transformations/T2_semantic_harmonization/configuration"
cdm_metadata <- as.data.table(readRDS(paste0(transformations_T2_semantic_harmonization_configuration,'CDM_metadata.rds')))

transformations_T2_semantic_harmonization_intermediate_data_file <- "/Users/ymao/Library/CloudStorage/OneDrive-UMCUtrecht/Work_UMC_230726/Paxlovid/Paxlovid-1047/transformations/T2_semantic_harmonization/intermediate_data_file"
dir_d2_db <- paste0(transformations_T2_semantic_harmonization_intermediate_data_file, 'd2.db')
db_connection_origin <- dbConnect(RSQLite::SQLite(), dir_d2_db)

data_D2_cdm <- "/Users/ymao/Library/CloudStorage/OneDrive-UMCUtrecht/Work_UMC_230726/Paxlovid/Paxlovid-1047/data/D2_cdm" 
cdm_table_names_load <- c('EVENTS','MEDICINES')
load_db(db_connection = db_connection_origin,csv_path_dir = data_D2_cdm,
          CDM_metadata = CDM_metadata,CDM_tables_names = CDM_tables_names)

#################
#OPTIONAL: Deleting duplicates 
################

delete_duplicates_flag <- TRUE
if (delete_duplicates_flag == TRUE){
  #The scheme defines the columns names (* = all) used for identifying unique records. The scheme is defined for every table in the CDM
  scheme <- setNames(rep("*",length(CDM_tables_names)),CDM_tables_names)
  delete_duplicates_origin(db_connection_origin, scheme, save.deleted = T, save.path = paste0(g_intermediate,'DeletedInputs/'), addPostFix = postFix)
}

#################
# Generate Unique ID of each record. 
################

create_unique_id(db_connection_origin, CDM_tables_names, extensionName = "", requireROWID = FALSE)

#################
#OPTIONAL: Report the number of rows per table, this is useful for early detection of missing records
################

count_rows_flag <- FALSE

if (count_rows_flag == TRUE){
  
  count_rows_origin <- get_rows_tables(db_connection_origin)
  print('[GetRowsTablesDataBase] Origin Database values')
  print(count_rows_OriginDB)
  
  dir_save_count_row <- "/Users/ymao/Library/CloudStorage/OneDrive-UMCUtrecht/Work_UMC_230726/Paxlovid/Paxlovid-1047/docs" # is it correct?
  saveRDS(count_rows_origin, dir_save_count_row)
}


dbDisconnect(db_connection_origin)

print(paste0('[Set Data Base]: IMPORT ACCOMPLISHED'))

