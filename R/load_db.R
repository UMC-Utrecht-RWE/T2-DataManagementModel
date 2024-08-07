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
load_db <- function(db_connection, csv_path_dir, cdm_metadata,
                    cdm_tables_names, extension_name = "") {
  # Loop through each table in cdm_tables_names
  for (table_name_pattern in cdm_tables_names) {
    table_metadata <- cdm_metadata[TABLE %in% table_name_pattern]
    date_cols <- table_metadata[stringr::str_detect(Format, 
                                                    "yyyymmdd") == TRUE & stringr::str_detect(Mandatory, 
                                                                                              "Yes") == TRUE, Variable]
    table_list <- list.files(csv_path_dir, pattern = table_name_pattern)
    indx_table <- 0
    for (c_table in table_list) {
      indx_table <- indx_table + 1
      existing_tables <- DBI::dbListTables(db_connection)
      if (!paste0(table_name_pattern, extension_name) %in% existing_tables) {
        print(paste0("[load_db]: Table ", table_name_pattern, " does not exist in the database and is created by loading the provided csv(s)"))
        add_new_table <- FALSE
      }else{
        add_new_table <- TRUE
      }
      
      print(paste0("[load_db]: Loading: ", c_table, " ", 
                   indx_table, "/", length(table_list)))
      cdm_loaded_table <- import_file(path = file.path(csv_path_dir, 
                                                       c_table))
      
      cols_input <- names(cdm_loaded_table)
      cols_selected <- cols_input[cols_input %in% table_metadata$Variable]
      cdm_loaded_table <- cdm_loaded_table[,c(cols_selected), with = F]
      
      existing_date_cols <- date_cols[date_cols %in% names(cdm_loaded_table)]
      missing_date_cols <- date_cols[!date_cols %in% names(cdm_loaded_table)]
      if (length(missing_date_cols) > 0) {
        print(paste0("[load_db]: Table: ", c_table, 
                     " is missing the following mandatory date columns: ", 
                     missing_date_cols))
      }
      if (length(existing_date_cols) > 0) {
        lapply(existing_date_cols, function(x) cdm_loaded_table[, 
                                                                `:=`(eval(x), as.Date(get(x), "%Y%m%d"))])
      }
      
      if (add_new_table == FALSE) {
        DBI::dbWriteTable(db_connection, paste0(table_name_pattern, 
                                                extension_name), cdm_loaded_table)
      }else {
        cols_in_table <- DBI::dbListFields(db_connection, 
                                           table_name_pattern)
        missing_in_input <- cols_in_table[!cols_in_table %in% 
                                            names(cdm_loaded_table)]
        missing_in_db <- names(cdm_loaded_table)[!names(cdm_loaded_table) %in% 
                                                   cols_in_table]
        if (length(missing_in_input) > 0) {
          print(paste0("[load_db]: The following columns were in previously loaded ",
                       table_name_pattern, " csvs but not in ", 
                       c_table, ": ",
                       missing_in_input))
          lapply(missing_in_input, function(new_column) {
            print(paste0("[load_db]: Adding column: ", 
                         new_column, " to table ", c_table))
            cdm_loaded_table[, `:=`(eval(new_column), NA)]
          })
        }
        if (length(missing_in_db) > 0) {
          print(paste0("[load_db]: The following columns are in ", c_table,
                       " and not in the previously loaded ", table_name_pattern, " csvs: ", 
                       paste(missing_in_db, collapse = ', ')))
          invisible(lapply(missing_in_db, function(new_column){
            print(paste0("[load_db]: Adding column: ", 
                         new_column, ' to table ', table_name_pattern, ' in the database'))
            if(class(db_connection)[1] %in% "duckdb_connection"){
              # Create a temporal_table with an additional column
              DBI::dbExecute(db_connection, paste0("CREATE TABLE temporal_table AS SELECT *, NULL ",new_column," FROM ", 
                                                   table_name_pattern))
              DBI::dbExecute(db_connection, paste0("ALTER TABLE temporal_table ALTER ",new_column," TYPE VARCHAR;"))
              
              # Drop the old table
              DBI::dbExecute(db_connection, paste0("DROP TABLE ", table_name_pattern))
              
              # Rename the temporal_table to the old table
              DBI::dbExecute(db_connection, paste0("ALTER TABLE temporal_table RENAME TO ", table_name_pattern))
              
            }else{
              DBI::dbExecute(db_connection, paste0("ALTER TABLE ", 
                                                   table_name_pattern," ADD COLUMN ",new_column," ;"))
            }
            
            
          }))
        }
        DBI::dbWriteTable(db_connection, paste0(table_name_pattern, 
                                                extension_name), cdm_loaded_table, overwrite = F, 
                          append = T)
      }
    }
  }
}

