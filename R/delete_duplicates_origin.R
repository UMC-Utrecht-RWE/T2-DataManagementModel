#' Delete Duplicate Rows in CDM Table of Origin Table
#'
#' This function removes duplicate rows from specified CDM tables based on a
#' given set of columns. Duplicates are identified using `rowid`, keeping the
#' first occurrence and deleting the rest.  
#'
#' The function can:
#' - Directly delete duplicates from the database.
#' - Optionally save deleted records as CSV files for tracking.
#' - Optionally create a view instead of deleting, keeping only distinct rows.
#'
#' @param db_connection Database connection object (SQLiteConnection or DuckDB connection).
#' @param scheme Named list where names are CDM table names and values are
#'   character vectors of columns to use for uniqueness. Use `"*"` to indicate
#'   all columns.
#' @param save_deleted Logical. If TRUE, deleted rows are saved as CSV files.
#'   Default is FALSE.
#' @param save_path Character. Directory path where deleted records are saved.
#'   Mandatory when `save_deleted = TRUE`.
#' @param add_postfix Optional character string. If provided, adds a postfix to
#'   the saved CSV file names. Default is `NA`.
#' @param schema_name Character. Name of the schema where tables are located.
#'   Default is `"main"`.
#' @param to_view Logical. If TRUE, instead of deleting rows, a view is created
#'   with distinct rows based on the specified columns. Default is FALSE.
#' @param pipeline_extension When using views when applying the clean_missing_values on CDM table we must define the name of the pipeline extension. See add_view for more information.
#' @param view_extension When using views when applying the clean_missing_values on CDM table we must define the name of the view. See add_view for more information.
#'
#' @examples
#' \dontrun{
#' # Example 1: Delete duplicates based on specific columns
#' db_connection <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
#' scheme <- list("EVENTS" = c("*"), "PERSONS" = c("person_id", "age"))
#' delete_duplicates_origin(db_connection, scheme)
#'
#' # Example 2: Delete duplicates across all columns in multiple tables and save deleted rows
#' scheme <- setNames(rep("*", length(CDM_tables_names)), CDM_tables_names)
#' delete_duplicates_origin(
#'   db_connection, scheme,
#'   save_deleted = TRUE,
#'   save_path = "/path/to/save"
#' )
#'
#' # Example 3: Create views with distinct rows instead of deleting
#' delete_duplicates_origin(
#'   db_connection, scheme,
#'   to_view = TRUE,
#'   view_prefix = "distinct_"
#' )
#' }
#'
#' @keywords internal

delete_duplicates_origin <- function(
    db_connection,
    scheme,
    save_deleted = FALSE,
    save_path = NULL,
    add_postfix = NA,
    schema_name = NULL,
    to_view = FALSE,
    pipeline_extension = '_T2DMM', 
    view_extension = '_T2DMM') {
  
  if(is.null(schema_name)){
    schema_name <- 'main'
  }
  
  valid_scheme <- list()
  # Check if specified columns in the scheme exist in the corresponding tables
  for (case_name in names(scheme)) {
    if (table_exists(con = db_connection, table_name = case_name, schema = schema_name) == TRUE) {
      
      col_names <- DBI::dbGetQuery(
        db_connection,
        sprintf(
          "SELECT column_name
           FROM information_schema.columns
           WHERE table_schema = %s AND table_name = %s
           ORDER BY ordinal_position",
          DBI::dbQuoteString(db_connection, schema_name),
          DBI::dbQuoteString(db_connection, case_name)
        )
      )
      if (!all(
        scheme[[case_name]] %in% col_names$column_name
      ) && all(!scheme[[case_name]] %in% "*")) {
        
        wrong_cols <- scheme[[case_name]][
          !scheme[[case_name]] %in% col_names$column_name
        ]
        message(paste0(
          "[delete_duplicates_origin]: Table ", case_name,
          " columns -> ", paste(wrong_cols, collapse = ", "),
          " do not exist in the DB instance table. Removing setting."
        ))
        
        next()
      }
      # Remove this entry from scheme
      valid_scheme[[case_name]] <- scheme[[case_name]]
    }
  }
  
  scheme <- valid_scheme
  
  
  if (length(scheme) > 0) {
    # Loop through each specified table in the scheme
    for (case_name in names(scheme)) {
      # Check if the table exists in the database
      if (case_name %in% DBI::dbListTables(db_connection)) {
        # Determine columns to select based on the scheme
        if (all(scheme[[case_name]] %in% "*")) {
          cols_to_select <- DBI::dbListFields(db_connection, case_name)
        } else {
          cols_to_select <- scheme[[case_name]]
        }
        cols_to_select <- paste(cols_to_select, collapse = ", ")
        
        # Build the SQL query to delete duplicate rows
        query <- paste0("DELETE FROM ", case_name, "
                      WHERE rowid NOT IN
                       (
                       SELECT  MIN(rowid)
                       FROM ",schema_name,".", case_name, "
                       GROUP BY ", cols_to_select, "
                       )")
        if(to_view == TRUE){
          pipeline_name <- paste0(case_name, pipeline_extension)
          final_alias   <- paste0(case_name, view_extension)
          query <-  paste0(
            "SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY ",cols_to_select,") AS rn
                FROM %s
            ) t
            WHERE rn = 1;
            "
          )
          T2.DMM:::add_view(con = db_connection, 
                            pipeline = pipeline_name, 
                            base_table = case_name, 
                            transform_sql = query, 
                            final_alias = final_alias)
        }
        # Execute the query and handle results
        if (save_deleted == FALSE & to_view == FALSE) {
          rs <- DBI::dbSendStatement(db_connection, query)
          DBI::dbHasCompleted(rs)
          num_rows <- DBI::dbGetRowsAffected(rs)
          message(paste0(
            "[delete_duplicates_origin] Number of record deleted: ",
            num_rows
          ))
          DBI::dbClearResult(rs)
        } else if (save_deleted == TRUE && !is.null(save_path) & to_view == FALSE) {
          rs <-  data.table::as.data.table(
            DBI::dbGetQuery(
              db_connection,
              paste0(query, " RETURNING *;")
            )
          )
          if (nrow(rs) > 0) {
            message(paste0(
              "[delete_duplicates_origin] Number of record deleted: ",
              nrow(rs)
            ))
            dir.create(file.path(save_path), showWarnings = FALSE)
            # Save deleted records with optional postfix
            if (!is.na(add_postfix) && is.character(add_postfix)) {
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
          } else {
            message(
              "[delete_duplicates_origin] Number of record deleted: 0"
            )
          }
        }
      }
    }
  }
}

#' Check if a table exists in a DuckDB database
#'
#' @param con A DBI connection to a DuckDB database.
#' @param table_name Name of the table to check.
#' @param schema Optional. Name of the schema to check within. Default is NULL, which checks all schemas.
#' @return TRUE if the table exists, FALSE otherwise.
table_exists <- function(con, table_name, schema = NULL) {
  if (!is.null(schema)) {
    sql <- sprintf(
      "SELECT COUNT(*) AS n
       FROM information_schema.tables
       WHERE table_schema = '%s' AND table_name = '%s'",
      schema, table_name
    )
  } else {
    sql <- sprintf(
      "SELECT COUNT(*) AS n
       FROM information_schema.tables
       WHERE table_name = '%s'",
      table_name
    )
  }
  
  result <- DBI::dbGetQuery(con, sql)$n
  return(result > 0)
}