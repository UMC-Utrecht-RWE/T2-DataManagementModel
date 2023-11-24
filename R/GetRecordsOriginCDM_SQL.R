#' Get Records from Origin CDM Tables within a DB Instance
#'
#' This function returns the records from the origin CDM tables within a DB instance that have a unique identifier.
#'
#' @param db_connection Database connection (SQLiteConnection).
#' @param UniqueID_dt Data.table containing unique identifiers.
#' @param UNIQUE_ID_NAME Name of the unique identifier. Default is "ID".
#' @param separatorID String that defines the separators between the table name and the ROWID number.
#'
#' @return A list containing records from the origin CDM tables for each unique identifier.
#'
#' @author Albert Cid Royo
#' @email a.cidroyo@umcutrecht.com
#' @organisation UMC Utrecht, Utrecht, The Netherlands
#' @date 16/01/2023
#'
#' @importFrom data.table data.table tstrsplit all.equal dbListFields dbGetQuery paste0 print next
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' table <- dbReadTable(db_connection, "EVENTS")
#' cases <- as.data.table(table)[seq(1, 1000), "ID"]
#' a <- GetRecordsOriginCDM_SQL(db_connection = db_connection, cases, separatorID = "-")
#' }
#'
#' @export
#'

GetRecordsOriginCDM_SQL <- function(db_connection, UniqueID_dt, UNIQUE_ID_NAME = "ID", separatorID = "-") {
  UniqueID_dt <- as.data.table(UniqueID_dt)
  returnValues <- list()

  # Check if the specified unique identifier exists in UniqueID_dt
  if (!UNIQUE_ID_NAME %in% names(UniqueID_dt)) {
    print(paste0("[GetRecordsOriginCDM_SQL] The unique identifier ", UNIQUE_ID_NAME, " Does not exist in the UniqueID_dt"))
    print("Returning empty list")
    return(returnValues)
  }

  # Split the unique identifier into CDM_TABLE and ROWID
  UniqueID_dt <- UniqueID_dt[, c("CDM_TABLE", "ROWID") := tstrsplit(UniqueID_dt[, get(UNIQUE_ID_NAME)], separatorID, fixed = TRUE)]

  # Check if separatorID is present in the unique identifier
  if (all.equal(UniqueID_dt$ID, UniqueID_dt$CDM_TABLE) == TRUE & all.equal(UniqueID_dt$ID, UniqueID_dt$ROWID) == TRUE) {
    print(paste0("[GetRecordsOriginCDM_SQL] The  ", separatorID, " does not exist in the unique identifier"))
    print("Returning empty list")
    return(returnValues)
  }

  # Loop through unique CDM_TABLEs
  for (table in unique(UniqueID_dt$CDM_TABLE)) {
    col_names_db <- dbListFields(db_connection, table)

    # Check if UNIQUE_ID_NAME exists in the table
    if (!UNIQUE_ID_NAME %in% col_names_db) {
      print(paste0("[GetRecordsOriginCDM_SQL] ", UNIQUE_ID_NAME, "does not exist in table ", table))
      print(paste0("Returning empty list for ", table))
      returnValues[[table]] <- list()
      next()
    }

    # Retrieve records from the table based on the unique identifier
    returnValues[[table]] <- as.data.table(dbGetQuery(db_connection, paste0(
      "SELECT *
      FROM ", table, " WHERE ", UNIQUE_ID_NAME, " IN (", paste0(UniqueID_dt[CDM_TABLE %in% table, UNIQUE_ID_NAME], collapse = ","), ")"
    )))
  }

  return(returnValues)
}
