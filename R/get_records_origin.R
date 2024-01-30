#' Get Records from Origin CDM Tables within a DB Instance
#'
#' This function returns the records from the origin CDM tables within a DB
#' instance that have a unique identifier.
#'
#' @param db_connection Database connection (SQLiteConnection).
#' @param unique_id_dt Data.table containing unique identifiers.
#' @param unique_id_name Name of the unique identifier. Default is "ID".
#' @param separator_id String that defines the separators between the table name
#'   and the ROWID number.
#'
#' @return A list containing records from the origin CDM tables for each unique
#'   identifier.
#'
#' @author Albert Cid Royo
#' @email a.cidroyo@umcutrecht.com
#' @organisation UMC Utrecht, Utrecht, The Netherlands
#' @date 16/01/2023
#'
#' @importFrom data.table data.table tstrsplit all.equal dbListFields dbGetQuery
#'   paste0 print next
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' table <- dbReadTable(db_connection, "EVENTS")
#' cases <- as.data.table(table)[seq(1, 1000), "ID"]
#' a <- GetRecordsOriginCDM_SQL(db_connection = db_connection, cases,
#'   separator_id = "-")
#' }
#'
#' @export
get_records_origin <- function(db_connection, unique_id_dt,
                                       unique_id_name = "ID", separator_id = "-") {
  unique_id_dt <- data.table::as.data.table(unique_id_dt)
  return_values <- list()
  
  # Check if the specified unique identifier exists in unique_id_dt
  if (!unique_id_name %in% names(unique_id_dt)) {
    print(paste0("[get_records_origin_cdm_sql] The unique identifier ",
                 unique_id_name, " Does not exist in the unique_id_dt"))
    print("Returning empty list")
    return(return_values)
  }
  
  # Split the unique identifier into cdm_table and rowid
  unique_id_dt <- unique_id_dt[, c("cdm_table", "rowid") :=
                                 data.table::tstrsplit(unique_id_dt[, get(unique_id_name)],
                                           separator_id, fixed = TRUE)]
  
  # Check if separator_id is present in the unique identifier
  if (all.equal(unique_id_dt$ID, unique_id_dt$cdm_table) == TRUE &&
      all.equal(unique_id_dt$ID, unique_id_dt$rowid) == TRUE) {
    print(paste0("[get_records_origin_cdm_sql] The  ", separator_id,
                 " does not exist in the unique identifier"))
    print("Returning empty list")
    return(return_values)
  }
  
  # Loop through unique cdm_tables
  for (table in unique(unique_id_dt$cdm_table)) {
    col_names_db <- DBI::dbListFields(db_connection, table)
    
    # Check if unique_id_name exists in the table
    if (!unique_id_name %in% col_names_db) {
      print(paste0("[get_records_origin_cdm_sql] ", unique_id_name,
                   "does not exist in table ", table))
      print(paste0("Returning empty list for ", table))
      return_values[[table]] <- list()
      next()
    }
    
    # Retrieve records from the table based on the unique identifier
    query <- paste0(
      "SELECT * FROM ", table, " WHERE ", unique_id_name, " IN (",
      paste0(unique_id_dt["cdm_table" %in% table, unique_id_name], collapse = ","),
      ")"
    )
    return_values[[table]] <- data.table::as.data.table(DBI::dbGetQuery(db_connection, query))
  }
  
  return(return_values)
}
