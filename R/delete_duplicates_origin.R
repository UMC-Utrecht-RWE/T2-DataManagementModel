#' Delete Duplicate Rows in CDM Table of origin table
#'
#' This function is designed for deleting duplicate rows based on the specified
#' columns.
#' The function deletes duplicate rows in the specified CDM tables and
#' optionally saves the deleted records.
#'
#' @param db_connection Database connection object (SQLiteConnection).
#' @param scheme List with CDM table names and character vectors with the
#'   columns/variables to be distinct. Use "*" for defining all columns.
#'   Special case for scheme if all variables/columns of a table are used.
#' @param save_deleted Logical, option to save the deleted rows. Good for
#'   tracking. Default is FALSE.
#' @param save_path Mandatory when save_deleted is TRUE. Path where the files
#'   with the deleted records will be saved.
#' @param add_postfix An additional postfix to the saved file name when saving
#'   deleted records. Default is NA.
#'
#' @examples
#' \dontrun{
#' # Example 1: Deleting duplicate rows in specified columns
#' db_connection <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' scheme <- list("EVENTS" = c("*"), "PERSONS" = c("person_id", "age"))
#' delete_duplicates_origin(db_connection, scheme)
#'
#' # Example 2: Deleting duplicate rows for all columns in specified tables
#' scheme <- setNames(rep("*", length(CDM_tables_names)), CDM_tables_names)
#' delete_duplicates_origin(db_connection, scheme,
#'   save_deleted = TRUE,
#'   save_path = "/path/to/save"
#' )
#' }
#'
#' @export
delete_duplicates_origin <- function(db_connection, scheme, save_deleted = FALSE,
                                     save_path = NULL, add_postfix = NA) {
  f_paste <- function(vec) sub(",\\s+([^,]+)$", " , \\1", toString(vec))

  # Check if specified columns in the scheme exist in the corresponding tables
  for (case_name in names(scheme)) {
    if (case_name %in% DBI::dbListTables(db_connection)) {
      if (!all(scheme[[case_name]] %in% DBI::dbListFields(db_connection, case_name)) &&
        all(!scheme[[case_name]] %in% "*")) {
        wrong_cols <- scheme[[case_name]][!scheme[[case_name]] %in%
          DBI::dbListFields(db_connection, case_name)]
        print(paste0(
          "[delete_duplicates_origin]: Table ", case_name,
          " columns -> ", wrong_cols,
          " do not exist in the DB instance table"
        ))
        stop()
      }
    }
  }

  # Loop through each specified table in the scheme
  for (case_name in names(scheme)) {
    # Check if the table exists in the database
    if (case_name %in% DBI::dbListTables(db_connection)) {
      # Determine columns to select based on the scheme
      if (all(scheme[[case_name]] %in% "*")) {
        cols_to_select <- DBI::dbListFields(db_connection, case_name)
        cols_to_select <- f_paste(cols_to_select)
      } else {
        cols_to_select <- scheme[[case_name]]
      }
      cols_to_select <- f_paste(cols_to_select)

      # Build the SQL query to delete duplicate rows
      query <- paste0("DELETE FROM ", case_name, "
                      WHERE rowid NOT IN
                       (
                       SELECT  MIN(rowid)
                       FROM ", case_name, "
                       GROUP BY ", cols_to_select, "
                       )")
      # Execute the query and handle results
      if (save_deleted == FALSE) {
        rs <- DBI::dbSendStatement(db_connection, query)
        DBI::dbHasCompleted(rs)
        num_rows <- DBI::dbGetRowsAffected(rs)
        print(paste0("[delete_duplicates_origin] Number of record deleted: ", num_rows))
        DBI::dbClearResult(rs)
      } else if (save_deleted == TRUE && !is.null(save_path)) {
        rs <- DBI::dbGetQuery(db_connection, paste0(query, " RETURNING *;"))
        rs <- data.table::as.data.table(rs)
        num_rows <- nrow(rs)
        print(paste0("[delete_duplicates_origin] Number of record deleted: ", num_rows))
        dir.create(file.path(save_path), showWarnings = FALSE) # Create folder
        # Save deleted records with optional postfix
        if (!is.na(add_postfix)) {
          save_file_name <- paste0(
            save_path, "/", case_name, "_",
            format(Sys.Date(), "%Y%m%d"), "_",
            add_postfix, ".csv"
          )
        } else {
          save_file_name <- paste0(
            save_path, "/", case_name, "_",
            format(Sys.Date(), "%Y%m%d"), ".csv"
          )
        }
        data.table::fwrite(rs, save_file_name)
      }
    }
  }
}
