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
#' @export
load_db <- function(db_connection, csv_path_dir, cdm_metadata,
                    cdm_tables_names, extension_name = "") {
  # Loop through each table in cdm_tables_names
  for (table in cdm_tables_names) {
    
    
    # What table are we going to read
    cat(paste0('Reading ', table, '...'))
    
    # Check if there is a file matching our table name
    matching_files <- list.files(
      path = csv_path_dir,
      pattern = paste0(table, ".*\\.csv$"),
      full.names = TRUE
    )
    
    # Skip this loop if there are no files to read
    if (length(matching_files) == 0) {
      cat(paste0("\rSkipping ", table, ", no files found.\n"))
      next
    }
    
    # The query that will read the data into DuckDB. The union_by_name = true will mean it will try to ignore differences
    # between files.
    path_files_with_patt <- file.path(csv_path_dir,table)
    query <- paste0('CREATE OR REPLACE TABLE ', table, ' AS
                    SELECT * FROM read_csv_auto("', path_files_with_patt, '*.csv", 
                    union_by_name = true, 
                    ALL_VARCHAR = true, 
                    nullstr = "NA" );')
 
    # Execute the query
    DBI::dbExecute(db_connection, query)
    
    # Yeah, done
    cat(paste0('\rFinished reading ', table, '.\n'))
    
    
    #Checking if any mandatory columns are missing within the database.
    cols_in_table <- DBI::dbListFields(db_connection,table)
    
    standard_cdm_table_columns <- unique(cdm_metadata[TABLE %in% table, Variable])
    mandatory_colums <- unique(cdm_metadata[TABLE %in% table & stringr::str_detect(Mandatory,"Yes") == TRUE, Variable])
    mandatory_missing_in_db <- unique(mandatory_colums[!mandatory_colums %in% cols_in_table])
    date_cols <- cdm_metadata[TABLE %in% table & stringr::str_detect(Format,"yyyymmdd") == TRUE, Variable]
    
    #If any mandatory colum is missing, then create it
    if (length(mandatory_missing_in_db) > 0) {
      print(paste0(
        "[load_db]: The following mandatory columns are missing in table: ", table
      ))
      print(paste(mandatory_missing_in_db, collapse = ", "))
      
      invisible(lapply(mandatory_missing_in_db, function(new_column) {
        DBI::dbExecute(db_connection, paste0(
          "ALTER TABLE ", table, " ADD COLUMN ", new_column, " VARCHAR;"
        ))
      }
      ))
    }
    
    additional_columns <- cols_in_table[!cols_in_table %in% standard_cdm_table_columns]
    print(paste0(
      "[load_db]: The following columns are not part of the CDM table but are in the files : ", additional_columns
    ))
    invisible(lapply(additional_columns, function(new_column) {
      DBI::dbExecute(db_connection, paste0(
        "ALTER TABLE ", table, " DROP COLUMN ", new_column, " ;"
      ))
      print(paste0(
        "[load_db]: Dropping table : ", new_column
      ))
    }))
    
    available_date_cols <- cols_in_table[cols_in_table %in% date_cols]
    invisible(lapply(available_date_cols, function(new_column) {
      tryCatch({
        # Attempt to change the column type to DATE directly
        DBI::dbExecute(db_connection, paste0(
          "ALTER TABLE ", table, " ALTER ", new_column, " TYPE DATE;"
        ))
      }, error = function(e) {
        # If direct conversion fails
        message("Direct conversion failed. Attempting to fix with STRPTIME.")
        
        # Nullify invalid values first
        DBI::dbExecute(db_connection, paste0(
          "UPDATE ", table, 
          " SET ", new_column, " = NULL WHERE ", new_column, " NOT SIMILAR TO '^[0-9]{8}$';"
        ))
        
        # Apply STRPTIME to reformat valid date strings
        DBI::dbExecute(db_connection, paste0(
          "UPDATE ", table, 
          " SET ", new_column, " = STRPTIME(", new_column, ", '%Y%m%d') WHERE ", new_column, " IS NOT NULL;"
        ))
        
        # Retry altering the column type to DATE
        DBI::dbExecute(db_connection, paste0(
          "ALTER TABLE ", table, " ALTER ", new_column, " TYPE DATE;"
        ))
        
        message("Column successfully converted to DATE after fixing format.")
      })
      
    }))
    
  }
}
