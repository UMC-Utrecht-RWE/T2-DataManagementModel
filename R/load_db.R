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
        print(paste0("[load_db]: Table created for first time: ", table_name_pattern ))
        add_new_table <- FALSE
      }else{
        add_new_table <- TRUE
      }
      
      print(paste0("[load_db]: Loading: ", c_table, " ", 
                   indx_table, "/", length(table_list)))
      cdm_loaded_table <- import_file(path = file.path(csv_path_dir, 
                                                       c_table))
      
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
          print(paste0("[load_db]: The following columns are missing in the NEW INPUT: ", 
                       missing_in_input))
          lapply(missing_in_input, function(new_column) {
            print(paste0("[load_db]: Addding column: ", 
                         new_column, ' to table ', table_name_pattern, ' to NEW INPUT'))
            cdm_loaded_table[, `:=`(eval(new_column), NA)]
          })
        }
        if (length(missing_in_db) > 0) {
          print(paste0("[load_db]: The following columns are missing in the DATABASE: ", 
                       paste(missing_in_db, collapse = ',')))
          invisible(lapply(missing_in_db, function(new_column){
            print(paste0("[load_db]: Addding column: ", 
                         new_column, ' to table ', table_name_pattern, ' in the DATABASE'))
            p <- DBI::dbSendStatement(db_connection, paste0("ALTER TABLE ", 
                                                            table_name_pattern," ADD COLUMN ",new_column," ;"))
            DBI::dbClearResult(p)
          }))
        }
        DBI::dbWriteTable(db_connection, paste0(table_name_pattern, 
                                                extension_name), cdm_loaded_table, overwrite = F, 
                          append = T)
      }
    }
  }
}

