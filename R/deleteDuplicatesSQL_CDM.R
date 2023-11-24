#' Delete Duplicate Rows in CDM Tables
#'
#' This function is designed for deleting duplicate rows based on the specified columns.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param scheme List with CDM table names and character vectors with the columns/variables to be distinct.
#'        Use "*" for defining all columns. Special case for scheme if all variables/columns of a table are used.
#' @param save.deleted Logical, option to save the deleted rows. Good for tracking. Default is FALSE.
#' @param save.path Mandatory when save.deleted is TRUE. Path where the files with the deleted records will be saved.
#' @param addPostFix An additional postfix to the saved file name when saving deleted records. Default is NA.
#'
#' @return The function deletes duplicate rows in the specified CDM tables and optionally saves the deleted records.
#'
#' @author Albert Cid Royo
#'
#' @importFrom DBI dbListTables dbListFields dbSendStatement dbGetQuery dbGetRowsAffected dbClearResult
#'
#' @examples
#' \dontrun{
#' # Example 1: Deleting duplicate rows in specified columns
#' db_connection <- dbConnect(RSQLite::SQLite(), ":memory:")
#' scheme <- list("EVENTS" = c("*"), "PERSONS" = c("person_id", "age"))
#' deleteDuplicatesSQL_CDM(db_connection, scheme)
#'
#' # Example 2: Deleting duplicate rows for all columns in specified tables
#' scheme <- setNames(rep("*", length(CDM_tables_names)), CDM_tables_names)
#' deleteDuplicatesSQL_CDM(db_connection, scheme, save.deleted = TRUE, save.path = "/path/to/save")
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

deleteDuplicatesSQL_CDM <- function(db_connection, scheme, save.deleted = FALSE, save.path = NULL, addPostFix = NA) {
  fPaste <- function(vec) sub(",\\s+([^,]+)$", " , \\1", toString(vec))

  # Check if specified columns in the scheme exist in the corresponding tables
  for (case_name in names(scheme)) {
    if (case_name %in% dbListTables(db_connection)) {
      if (!all(scheme[[case_name]] %in% dbListFields(db_connection, case_name)) & all(!scheme[[case_name]] %in% "*")) {
        wrongCols <- scheme[[case_name]][!scheme[[case_name]] %in% dbListFields(db_connection, case_name)]
        print(paste0("[DeleteDuplicatesSQL]: Table ", case_name, " columns -> ", wrongCols, " do not exist in the DB instance table"))
        stop()
      }
    }
  }

  # Loop through each specified table in the scheme
  for (case_name in names(scheme)) {
    # Check if the table exists in the database
    if (case_name %in% dbListTables(db_connection)) {
      # Determine columns to select based on the scheme
      if (all(scheme[[case_name]] %in% "*")) {
        colsToSelect <- dbListFields(db_connection, case_name)
        colsToSelect <- fPaste(colsToSelect)
      } else {
        colsToSelect <- scheme[[case_name]]
      }
      colsToSelect <- fPaste(colsToSelect)

      # Build the SQL query to delete duplicate rows
      query <- paste0("DELETE FROM ", case_name, "
                                          WHERE rowid NOT IN
                                           (
                                           SELECT  MIN(rowid)
                                           FROM ", case_name, "
                                           GROUP BY ", colsToSelect, "
                                           )
                                             ")
      # Execute the query and handle results
      if (save.deleted == FALSE) {
        rs <- dbSendStatement(db_connection, query)
        dbHasCompleted(rs)
        numRows <- dbGetRowsAffected(rs)
        dbClearResult(rs)
      } else if (save.deleted == TRUE & !is.null(save.path)) {
        rs <- dbGetQuery(db_connection, paste0(query, " RETURNING *;"))
        rs <- as.data.table(rs)
        numRows <- nrow(rs)
        dir.create(file.path(save.path), showWarnings = FALSE) # Create folder
        # Save deleted records with optional postfix
        if (!is.na(addPostFix)) {
          saveRDS(rs, paste0(save.path, case_name, "_deleted", addPostFix, ".rds"))
        } else {
          saveRDS(rs, paste0(save.path, case_name, "_deleted.rds"))
        }
        rm(rs)
      } else {
        stop("[DeleteDuplicatesSQL] Error in the input variables. Check requirements.")
      }
      # Print information about the operation
      print(paste0("[DeleteDuplicatesSQL] Table: ", case_name))
      print(paste0("Number of duplicated rows deleted: ", numRows))
    } else {
      print(paste0("[DeleteDuplicatesSQL] Table: ", case_name, " does not exist in DB"))
    }
  }
}
