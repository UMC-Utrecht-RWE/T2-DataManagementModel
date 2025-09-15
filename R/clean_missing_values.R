#' Clean tables by removing rows with missing values
#'
#' For each table listed in \code{list_columns_clean}, this function either:
#' - creates or extends a view pipeline (non-destructive, dynamic), OR
#' - materializes a cleaned version by overwriting the original table.
#'
#' Missing values are defined as \code{NULL}, empty string (\code{''}), or the string \code{'NA'}.
#'
#' @param con A DBI connection to a DuckDB database.
#' @param list_columns_clean A named list. Each element is a character vector of column
#'   names to check for missing values. Names of the list must match table names in the database.
#' @param mode Either \code{"view"} (default) or \code{"materialized"}.
#'   - In \code{"view"} mode, a pipeline of views is created using \code{add_view()}.
#'   - In \code{"materialized"} mode, the original table is replaced with a cleaned version.
#' @param log_fun Optional logging function. Defaults to \code{message}.
#'
#' @return Invisibly returns \code{TRUE} after processing all tables.
#'
#' @examples
#' \dontrun{
#' list_cols <- list(
#'   PERSONS = c("person_id"),
#'   VACCINES = c("person_id")
#' )
#' 
#' # Create dynamic views
#' clean_missing_values(con, list_cols, mode = "view")
#'
#' # Overwrite tables with materialized cleaned version
#' clean_missing_values(con, list_cols, mode = "materialized")
#' }
clean_missing_values <- function(con, list_columns_clean, mode = c("view", "materialized"), log_fun = message) {
  mode <- match.arg(mode)
  
  # Get available tables
  tables_available <- DBI::dbListTables(con)
  
  for (table_to_clean in names(list_columns_clean)) {
    if (table_to_clean %in% tables_available) {
      log_fun(sprintf("[Processing table] %s (%s mode)", table_to_clean, mode))
      
      for (colname in list_columns_clean[[table_to_clean]]) {
        filter_sql <- sprintf(
          "%s IS NOT NULL AND %s <> '' AND %s <> 'NA'",
          colname, colname, colname
        )
        
        if (mode == "view") {
          # ---- Dynamic pipeline of views ----
          transform_sql <- sprintf("SELECT * FROM %%s WHERE %s", filter_sql)
          pipeline_name <- paste0(table_to_clean, "_pipeline")
          final_alias   <- paste0(table_to_clean, "_view")
          
          add_view(
            con = con,
            pipeline = pipeline_name,
            base_table = table_to_clean,   # only used if pipeline not initialized
            transform_sql = transform_sql,
            final_alias = final_alias
          )
          
        } else if (mode == "materialized") {
          # ---- Overwrite original table with cleaned version ----
          tmp_table <- paste0(table_to_clean, "_tmp_clean")
          
          sql_create <- sprintf(
            "CREATE OR REPLACE TABLE %s AS
             SELECT * FROM %s WHERE %s",
            tmp_table, table_to_clean, filter_sql
          )
          DBI::dbExecute(con, sql_create)
          
          # Replace original with cleaned
          DBI::dbExecute(con, sprintf("DROP TABLE %s", table_to_clean))
          DBI::dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", tmp_table, table_to_clean))
          
          log_fun(sprintf("  -> Table %s overwritten with cleaned version", table_to_clean))
        }
      }
    } else {
      log_fun(sprintf("Table %s does not exist", table_to_clean))
    }
  }
  
  invisible(TRUE)
}
