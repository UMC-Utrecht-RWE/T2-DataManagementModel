#' Load CDM Data Set Instance from CSV Files into a Database
#'
#' This function is designed for loading a CDM data set instance from CSV files
#' into a database.
#'
#' @param db_connection A database connection object (SQLiteConnection).
#' @param csv_path_dir Path to the CSV CDM database.
#' @param cdm_metadata Data.table with Table name, Variable name, and Format.
#' @param cdm_tables_names List of CDM tables names to be imported into the DB.
#' @param extension_name String to be added to the name of the tables, useful
#'   when loading different CDM instances in the same DB.
#'
#' @return No direct return value. Tables are loaded into the specified database.
#'
#' @author Albert Cid Royo
#'
#' @importFrom DBI dbListTables dbGetQuery dbListFields dbWriteTable
#'   dbSendStatement
#' @import data.table rlist
#'
#' @examples
#' \dontrun{
#' # Example usage of load_db
#' db <- dbConnect(RSQLite::SQLite(), ":memory:")
#' cdm_metadata <- data.table(TABLE = c("table1", "table2"),
#'   Variable = c("var1", "var2"), Format = c("yyyymmdd", "numeric"))
#' cdm_tables_names <- c("table1", "table2")
#' load_db(db, "path/to/csv/files/", cdm_metadata, cdm_tables_names,
#'   "instance1")
#' }
#'
#' @export
#' @keywords database
load_db <- function(db_connection, csv_path_dir, cdm_metadata,
                    cdm_tables_names, extension_name = "") {
  # Loop through each table in cdm_tables_names
  for (table_name_pattern in cdm_tables_names) {
    print(paste0("[load_db]: Loading tables: ", table_name_pattern))
    # Get metadata for the current table
    table_metadata <- cdm_metadata[TABLE %in% table_name_pattern]
    # Identify date columns that are mandatory and date
    date_cols <- table_metadata[str_detect(Format, "yyyymmdd") == TRUE &
                                  str_detect(Mandatory, "Yes") == TRUE, Variable]
    # Get a list of CSV files for the current table
    table_list <- list.files(csv_path_dir, pattern = table_name_pattern)
    indx_table <- 0
    # Loop through each CSV file for the current table
    for (c_table in table_list) {
      indx_table <- indx_table + 1
      print(paste0("[load_db]: Loading: ", c_table, " ", indx_table, "/",
                   length(table_list)))
      # If there are mandatory date columns, import the file with the date columns
      cdm_loaded_table <- import_file(paste0(csv_path_dir, c_table))
      
      # Set specified columns in data.colls to date format
      existing_date_cols <- date_cols[date_cols %in% names(cdm_loaded_table)]
      missing_date_cols <- date_cols[!date_cols %in% names(cdm_loaded_table)]
      if (length(missing_date_cols) > 0) {
        print(paste0("[load_db]: Table: ", c_table,
                     " is missing the following mandatory date columns: ",
                     missing_date_cols))
      }
      if (length(existing_date_cols) > 0) {
        lapply(existing_date_cols, function(x) cdm_loaded_table[, eval(x) :=
                                                                  as.Date(get(x), "%Y%m%d")])
      }
      
      # Get a list of existing tables in the database
      existing_tables <- DBI::dbListTables(db_connection)
      
      # If the table does not exist in the database, create and write the table
      if (!paste0(table_name_pattern, extension_name) %in% existing_tables) {
        DBI::dbWriteTable(db_connection, paste0(table_name_pattern,
                                                extension_name),
                          cdm_loaded_table)
      } else {
        # If the table exists, check for missing columns in the input and database
        cols_in_table <- DBI::dbListFields(db_connection, table_name_pattern)
        missing_in_input <- cols_in_table[!cols_in_table %in%
                                            names(cdm_loaded_table)]
        missing_in_db <- names(cdm_loaded_table)[!names(cdm_loaded_table) %in%
                                                   cols_in_table]
        
        # If there are missing columns, print a warning
        if (length(missing_in_input) > 0) {
          print(paste0("[load_db]: The following columns are missing in the input: ",
                       missing_in_input))
        }
        # If there are missing columns in the database, add the columns
        if (length(missing_in_db) > 0) {
          rs <-DBI::dbSendStatement(db_connection, paste0("ALTER TABLE ", table_name_pattern, paste0(" ADD COLUMN ", missing_in_db, collapse = ""), " ;"))
          DatabaseConnector::dbClearResults(rs)
        }
        
        # If there are missing columns in the input, add the columns with NA values
        if (length(missing_in_input) > 0) {
          lapply(missing_in_input, function(x) CDM_loaded_table[, eval(x) := NA])
        }
        
        # Append the data to the existing table
        DBI::dbWriteTable(db_connection, paste0(table_name_pattern, extension_name), CDM_loaded_table, overwrite = F, append = T)
      }
    }
  }
}

