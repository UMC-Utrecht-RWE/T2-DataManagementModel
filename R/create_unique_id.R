#' Create Unique IDs for CDM Tables
#'
#' This function generates a unique CDM ID for each data record of the specified 
#' Database CDM instance. This step is suggested to be applied after using the 
#' loadDb function.
#' The function modifies the specified database by creating unique IDs 
#' for the specified CDM tables.
#'
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param cdm_tables_names List of CDM tables names to be imported into the database.
#' @param extension_name String to be added to the name of the tables, useful when 
#' loading different CDM instances in the same database.
#' @param id_name String that defines the name of the unique identifier. 
#' By default is set as "Ori_ID".
#' @param separator_id String that defines the separators between the table name 
#' and the ROWID number.
#'
#' @examples
#' \dontrun{
#' # Example usage of create_unique_id
#' db_connection <- dbConnect(RSQLite::SQLite(), ":memory:")
#' cdm_tables_names <- c("PERSONS", "VISITS", "OBSERVATIONS")
#' create_unique_id(db_connection, cdm_tables_names, extension_name = "_CDM1", 
#' id_name = "CDM_ID", separator_id = "_", require_rowid = FALSE)
#' }
#'
#' @export
create_unique_id <- function(db_connection, cdm_tables_names, 
                                 extension_name = "", id_name = "Ori_ID", 
                                 separator_id = "-") {
  # Append the extension to CDM table names
  cdm_tables_names <- paste0(cdm_tables_names, extension_name)
  
  # Retrieve the names of all tables in the database
  list_existing_tables <- DBI::dbListTables(db_connection)
  
  # Get the existing tables among the specified CDM tables
  cdm_tables_names_existing <- cdm_tables_names[cdm_tables_names %in% 
                                                  list_existing_tables]
  
  # Check if tables exist in the database
  if (length(cdm_tables_names[!cdm_tables_names %in% list_existing_tables]) > 0) {
    print(paste0("[CreateUniqueIDCDM] Can not create unique IDs on the following ",
                 "CDM table because they do not exist in the database "))
    print(cdm_tables_names[!cdm_tables_names %in% list_existing_tables])
  }
  
  # Loop through each existing CDM table
  for (table in cdm_tables_names_existing) {
    
    # Rename the table and create a new one with the unique identifier
    
    DBI::dbExecute(db_connection, paste0("CREATE TABLE temporal_table AS
                                      SELECT  '", table, separator_id, 
                                          "' || rowid AS ", id_name, ",
                                      '", table, "' AS Ori_Table, 
                                      rowid AS ROWID, *
                                      FROM ", table), n = -1)
    
    DBI::dbExecute(db_connection, paste0("DROP TABLE ", table), n = -1)
    
    DBI::dbExecute(db_connection, paste0("ALTER TABLE temporal_table RENAME TO ", table))
    print(paste0('[CreateUniqueIDCDM] Unique ID create for table: ',table))
  }
}
