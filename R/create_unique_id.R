#' Create Unique IDs for CDM Tables
#'
#' This function generates a unique CDM ID for each data record of the specified 
#' Database CDM instance. This step is suggested to be applied after using the 
#' loadDb function.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param cdm_tables_names List of CDM tables names to be imported into the database.
#' @param extension_name String to be added to the name of the tables, useful when 
#' loading different CDM instances in the same database.
#' @param id_name String that defines the name of the unique identifier. 
#' By default is set as "Ori_ID".
#' @param separator_id String that defines the separators between the table name 
#' and the ROWID number.
#' @param require_rowid Logical, default is FALSE. If TRUE, the unique ID won't 
#' be generated unless the ROWID column exists in the table.
#'
#' @return The function modifies the specified database by creating unique IDs 
#' for the specified CDM tables.
#'
#' @author Albert Cid Royo
#'
#' @importFrom DBI dbListTables dbListFields dbSendStatement
#'
#' @examples
#' \dontrun{
#' # Example usage of create_unique_id_cdm
#' db_connection <- dbConnect(RSQLite::SQLite(), ":memory:")
#' cdm_tables_names <- c("PERSONS", "VISITS", "OBSERVATIONS")
#' create_unique_id_cdm(db_connection, cdm_tables_names, extension_name = "_CDM1", 
#' id_name = "CDM_ID", separator_id = "_", require_rowid = FALSE)
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

create_unique_id <- function(db_connection, cdm_tables_names, 
                                 extension_name = "", id_name = "Ori_ID", 
                                 separator_id = "-", require_rowid = FALSE) {
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
                 "CDM table because they do not exist in the database ", 
                 tail(unlist(str_split(db_connection@dbname, "/")), 1)))
    print(cdm_tables_names[!cdm_tables_names %in% list_existing_tables])
  }
  
  # Loop through each existing CDM table
  for (table in cdm_tables_names_existing) {
    # Get columns of the current table
    columns_db_table <- DBI::dbListFields(db_connection, table)
    
    # Check if "row_names" exists in the table and require_rowid is FALSE
    if (!"row_names" %in% columns_db_table & require_rowid == FALSE) {
      print(paste0("[CreateUniqueIDCDM] row_names for table ", table, 
                   " does not exist"))
      print(paste0("row_names is created in ", table))
      
      # Rename the table and create a new one with ROWID as row_names
      rs <- DBI::dbSendStatement(db_connection, paste0("ALTER TABLE ", table, 
                                            " RENAME TO OLD_", table, ";"), n = -1)
      DBI::dbClearResult(rs)
      
      rs <- DBI::dbSendStatement(db_connection, paste0(
        "CREATE TABLE ", table,
        " AS SELECT ROWID as row_names, * FROM OLD_", table
      ), n = -1)
      DBI::dbClearResult(rs)
      rs <- DBI::dbSendStatement(db_connection, paste0("DROP TABLE OLD_", table), n = -1)
      DBI::dbClearResult(rs)
    } else if (!"row_names" %in% columns_db_table && require_rowid == TRUE) {
      # If require_rowid is TRUE and "row_names" doesn't exist, print a message 
      # and skip to the next table
      print(paste0(print("[CreateUniqueIDCDM] row_names for table ", table, 
                         " does not exist")))
      print("UNIQUE ID is not generated. Define require_rowid to FALSE if you 
        want to generate the UNIQUE ID.")
      next()
    }
    
    # Rename the table and create a new one with the unique identifier
    rs <- DBI::dbSendStatement(db_connection, paste0("ALTER TABLE ", table, 
                                          " RENAME TO OLD_", table, ";"), n = -1)
    DBI::dbClearResult(rs)
    
    rs <- DBI::dbSendStatement(db_connection, paste0("CREATE TABLE ", table, ' AS
                                      SELECT  "', table, separator_id, 
                                          '" || row_names AS ', id_name, ',
                                      "', table, '" AS Ori_Table, 
                                      row_names as ROWID, *
                                      FROM OLD_', table), n = -1)
    DBI::dbClearResult(rs)
    
    rs <- DBI::dbSendStatement(db_connection, paste0("ALTER TABLE ", table, 
                                          " DROP COLUMN row_names ;"), n = -1)
    DBI::dbClearResult(rs)
    
    rs <- DBI::dbSendStatement(db_connection, paste0("DROP TABLE OLD_", table), n = -1)
    DBI::dbClearResult(rs)
  }
}
