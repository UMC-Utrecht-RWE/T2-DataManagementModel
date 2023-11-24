#' Import DAP-Specific Codelist to Database
#'
#' This function reads a DAP-specific codelist from a file, selects specified columns, and imports it into a database table.
#'
#' @param codelist_path Path to the DAP-specific codelist file.
#' @param codelistName_db Name of the database table where the codelist will be imported.
#' @param databaseConnection Database connection object (e.g., SQLiteConnection).
#' @param columns Vector of column names to select and import from the codelist.
#'
#' @return None (data is imported into the specified database table).
#'
#' @author Albert Cid Royo
#'
#' @importFrom data.table as.data.table setnames unique lapply DBI dbWriteTable readRDS
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' codelist_path <- "path/to/codelist.rds"
#' codelistName_db <- "CodelistTable"
#' db_connection <- dbConnect(RSQLite::SQLite(), "your_database.db")
#' columns_to_import <- c("code", "description") # Add desired column names
#' importDapSpecificCodelist(codelist_path, codelistName_db, db_connection, columns_to_import)
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

importDapSpecificCodelist <- function(codelist_path, codelistName_db, databaseConnection, columns) {
  # Read the DAP-specific codelist from the file
  codelist <- as.data.table(readRDS(file = codelist_path))

  # Select specified columns and unique rows
  codelist <- unique(codelist[Comment %in% "BOTH", ..columns])

  # Convert selected columns to uppercase
  lapply(columns, function(x) codelist[, eval(x) := toupper(get(x))])

  # Write the codelist to the specified database table
  dbWriteTable(databaseConnection, codelistName_db, codelist, overwrite = TRUE)
}
