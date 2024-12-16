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
load_db <- function(db_connection, csv_path_dir, cdm_name = NULL, cdm_metadata, cdm_schema_sqlquery = NULL,
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
                    nullstr = ["NA"," "] );')
    
    # Execute the query
    DBI::dbExecute(db_connection, query)
    
    list_columns_table <- dbListFields(db_connection,table)
    for(column_name in list_columns_table){
      update_query <- paste0('UPDATE ', table, ' 
                        SET ',column_name,' = NULL 
                        WHERE ',column_name,' = \'\';')
      # Execute the query
      DBI::dbExecute(db_connection, update_query)
    }
    
    # Yeah, done
    cat(paste0('\rFinished reading ', table, '.\n'))
  }
  
  if(!is.null(cdm_schema_sqlquery)){
    # Create the CDM schema
    dbExecute(db_connection, paste0("CREATE SCHEMA IF NOT EXISTS ", cdm_name))
    dbExecute(db_connection, cdm_schema_sqlquery)
    
    # Small function to read the columns and datatypes per table
    get_table_info <- function(schema, table) {
      dbGetQuery(db_connection, paste0(
        "SELECT column_name, data_type 
     FROM information_schema.columns 
     WHERE table_schema = '", schema, "' AND table_name = '", table, "'"
      ))
    }
    
    # Get the list of tables in both schemas
    tables_main <- dbGetQuery(db_connection, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
    tables_cdm <- dbGetQuery(db_connection, paste0("SELECT table_name FROM information_schema.tables WHERE table_schema = '",cdm_name,"'"))
    
    # We only want to read the tables that are in both the import and the cdm schema
    common_tables <- intersect(tables_main$table_name, tables_cdm$table_name)
    
    # Loop through each table
    for (table in common_tables) {
      
      # Some verbosity
      cat('Start transfer of', table, '....')
      
      # Execute the get_table_info for that table, for both schemas
      cols_main <- get_table_info("main", table)
      cols_cdm <- get_table_info(cdm_name, table)
      
      # Intersect the columns from each schema, so we only use the columns that are both in import and target schema
      common_columns <- intersect(cols_main$column_name, cols_cdm$column_name)
      
      # Here is the magic, we're reading and try_casting to the target datatype. If we cannot cast we will ignore the value
      # and move on to the next value. This is an explicit cleanup....
      # I don't really like creating SQL code from R but I don't really see another way....
      query <- paste0(
        "INSERT INTO ", cdm_name,".", table, " (", paste(common_columns, collapse = ", "), ") ",
        "SELECT ", paste(
          sapply(common_columns, function(col) {
            target_type <- cols_cdm$data_type[cols_cdm$column_name == col]
            paste0("TRY_CAST(", col, " AS ", target_type, ") AS ", col)
          }),
          collapse = ", "
        ), 
        " FROM main.", table
      )
      
      # Execute the query
      dbExecute(db_connection, query)
      
      #Delete the imported table
      dbExecute(db_connection, paste0("DROP TABLE IF EXISTS main.", table))
      
      # And we're ready
      cat("\rDone loading the target table:", table, "\n")
    }
    
    
  }else{
    print('Transfer completed')
  }
}
