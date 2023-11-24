#' Import DAP-Specific Concepts into Database
#'
#' This function imports DAP-specific concepts into a database, applying necessary transformations based on the provided codelist.
#'
#' @param codelist A data.table containing information about tables, columns, and values for DAP-specific concepts.
#' @param data_db The database connection object.
#' @param nameAttachment Attachment to the database table names.
#' @param save_db The database connection object where the edited tables and concepts will be saved.
#' @param case_insensitive Logical, indicating whether column names are case-insensitive. Default is FALSE.
#' @param date_col_filter An optional filter to subset data based on a specified date column.
#'
#' @return The function modifies the specified save_db by creating edited tables and importing DAP-specific concepts.
#'
#' @author Albert Cid Royo
#'
#' @importFrom data.table fread setnames dbListTables dbListFields dbSendStatement
#'
#' @examples
#' \dontrun{
#' # Example usage of importDapSpecificConcepts_db
#' codelist <- fread("path/to/codelist.csv")
#' data_db <- dbConnect(RSQLite::SQLite(), ":memory:")
#' save_db <- dbConnect(RSQLite::SQLite(), ":memory:")
#' importDapSpecificConcepts_db(codelist, data_db, "Attachment", save_db, case_insensitive = FALSE, date_col_filter = "20230101")
#' }
#'
#' @export
#' @keywords database
#' @name RWEDataManagementModel
#' @docType package
#'

importDapSpecificConcepts_db <- function(codelist, data_db, nameAttachment, save_db, case_insensitive = FALSE, date_col_filter = NULL) {
  # Check if codelist is not empty
  if (nrow(codelist) > 0) {
    # Get unique tables from codelist
    scheme <- unique(codelist[["table"]])
    # Get columns and value names
    cols_names <- colnames(codelist)[substr(colnames(codelist), 1, 3) == "col"]
    value_names <- colnames(codelist)[substr(colnames(codelist), 1, 3) == "val"]
    # Get columns and values
    cols <- codelist[, ..cols_names]
    values <- codelist[, ..value_names]

    # Preprocess all possible tables:
    # Loop through each table in scheme
    for (name in scheme) {
      # Edit table name to indicate that it's been processed
      name_edited <- paste0(name, "_EDITED")
      # Get columns that need to be converted to uppercase
      to_upper_cols <- na.omit(unique(unlist(codelist[table %in% name, ..cols_names])))
      # Get all columns from the table
      columns_db_table <- dbListFields(data_db, name)
      # Get columns that don't need to be converted to uppercase
      rest_cols <- columns_db_table[!columns_db_table %in% to_upper_cols]
      # Create query to convert columns to uppercase
      to_upper_query <- paste0(paste0("UPPER(", to_upper_cols, ") AS ", to_upper_cols), collapse = ", ")
      # Create query to select all columns and that are not converted to uppercase
      select_cols_query <- paste0(paste0(rest_cols, collapse = ", "), " ,")
      # If edited table doesn't exist in save_db, create a temporary table
      if (!name_edited %in% dbListTables(save_db)) {
        dbSendStatement(save_db, paste0("CREATE TEMP TABLE ", name_edited, " AS
              SELECT ", select_cols_query, " ", to_upper_query, "
              FROM ", nameAttachment, ".", name))
      } else if (all(c(rest_cols, to_upper_cols) %in% dbListFields(save_db, name_edited)) == FALSE) {
        # If the edited database exists but not all the columns have been included, you need to import the table again
        dbSendStatement(save_db, paste0("CREATE TEMP TABLE ", name_edited, " AS
              SELECT ", select_cols_query, " ", to_upper_query, "
              FROM ", nameAttachment, ".", name))
      }
    }

    # Loop through each row in codelist
    for (j in 1:nrow(codelist)) {
      # Get table name, edited table name, concept name, date column, columns, and values
      table_temp <- codelist[[j, "table"]]
      name_edited <- paste0(table_temp, "_EDITED")
      concept_name <- codelist[[j, "Outcome"]]
      date_col <- codelist[[j, "date_column"]]
      codelist_ID <- codelist[[j, "DAP_SPEC_ID"]]
      cols_temp <- na.omit(as.character(cols[j]))
      values_temp <- toupper(na.omit(as.character(values[j])))
      value <- codelist[[j, "keep"]]
      if (is.na(value) == TRUE) {
        value <- " TRUE "
      }
      # Create coding system name
      coding_system <- paste0("'", codelist_ID, "'")
      # Create where statement for the query
      where_statement <- paste(paste(cols_temp, paste0("'", values_temp, "'"), sep = " = "), collapse = " AND ")
      if (!is.null(date_col_filter)) {
        where_statement <- paste0(where_statement, " AND ", date_col, " >= ", as.integer(date_col_filter))
      }

      # Insert data into the concept_table in save_db
      dbSendStatement(save_db, paste0(
        "INSERT INTO concept_table
                                   SELECT Ori_ID, Ori_Table, ROWID, person_id, ", coding_system, " AS code, ", coding_system, " AS coding_system, ", value, " AS value, '", concept_name, "' AS Outcome, ", date_col, " AS date ", "
                                   FROM ", name_edited,
        " WHERE ", where_statement
      ))
    }
  }
}
