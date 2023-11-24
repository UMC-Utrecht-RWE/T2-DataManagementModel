#' Create Unique IDs for CDM Tables
#'
#' This function generates a unique CDM ID for each data record of the specified Database CDM instance.
#' This step is suggested to be applied after using the CSV_to_DB function.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param CDM_tables_names List of CDM tables names to be imported into the database.
#' @param extensionName String to be added to the name of the tables, useful when loading different CDM instances in the same database.
#' @param ID_NAME String that defines the name of the unique identifier. By default is set as "Ori_ID".
#' @param separatorID String that defines the separators between the table name and the ROWID number.
#' @param requireROWID Logical, default is FALSE. If TRUE, the unique ID won't be generated unless the ROWID column exists in the table.
#'
#' @return The function modifies the specified database by creating unique IDs for the specified CDM tables.
#'
#' @author Albert Cid Royo
#'
#' @importFrom DBI dbListTables dbListFields dbSendStatement
#'
#' @examples
#' \dontrun{
#' # Example usage of CreateUniqueID_CDM
#' db_connection <- dbConnect(RSQLite::SQLite(), ":memory:")
#' CDM_tables_names <- c("PERSONS", "VISITS", "OBSERVATIONS")
#' CreateUniqueID_CDM(db_connection, CDM_tables_names, extensionName = "_CDM1", ID_NAME = "CDM_ID", separatorID = "_", requireROWID = FALSE)
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

CreateUniqueID_CDM <- function(db_connection, CDM_tables_names, extensionName = "", ID_NAME = "Ori_ID", separatorID = "-", requireROWID = FALSE) {
  CDM_tables_names <- paste0(CDM_tables_names, extensionName)
  listExistingTables <- dbListTables(db_connection)
  CDM_tables_names_existing <- CDM_tables_names[CDM_tables_names %in% listExistingTables]

  if (length(CDM_tables_names[!CDM_tables_names %in% listExistingTables]) > 0) {
    print(paste0("[CreateUniqueIDCDM] Can not create unique IDs on the following CDM table because they do not exist in the database ", tail(unlist(str_split(db_connection@dbname, "/")), 1)))
    print(CDM_tables_names[!CDM_tables_names %in% listExistingTables])
  }

  for (table in CDM_tables_names_existing) {
    columns_db_table <- dbListFields(db_connection, table)

    if (!"row_names" %in% columns_db_table & requireROWID == FALSE) {
      print(paste0("[CreateUniqueIDCDM] row_names for table ", table, "does not exist"))
      print(paste0("row_names is created in ", table))
      dbSendStatement(db_connection, paste0("ALTER TABLE ", table, " RENAME TO OLD_", table, ";"), n = -1)
      dbSendStatement(db_connection, paste0(
        "CREATE TABLE ", table,
        " AS SELECT ROWID as row_names,*
                                              FROM OLD_", table
      ), n = -1)
      dbSendStatement(db_connection, paste0("DROP TABLE OLD_", table), n = -1)
    } else if (!"row_names" %in% columns_db_table & requireROWID == TRUE) {
      print(paste0(print("[CreateUniqueIDCDM] row_names for table ", table, "does not exist")))
      print("UNIQUE ID is not generated. Define requireROWID to FALSE if you want to generate the UNIQUE ID.")
      next()
    }
    dbSendStatement(db_connection, paste0("ALTER TABLE ", table, " RENAME TO OLD_", table, ";"), n = -1)
    dbSendStatement(db_connection, paste0("CREATE TABLE ", table, ' AS
                                          SELECT  "', table, separatorID, '" || row_names AS ', ID_NAME, ',"', table, '" AS Ori_Table, row_names as ROWID, *
                                          FROM OLD_', table), n = -1)
    dbSendStatement(db_connection, paste0("ALTER TABLE ", table, " DROP COLUMN row_names ;"), n = -1)
    dbSendStatement(db_connection, paste0("DROP TABLE OLD_", table), n = -1)
  }
}
