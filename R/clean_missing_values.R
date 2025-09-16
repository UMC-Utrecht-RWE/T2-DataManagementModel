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
#' @param to_view Logical. If `TRUE` (default), creates a view with the unique ID column.
#' If `FALSE`, overwrites the original table.
#' @param pipeline_extension When using views when applying the clean_missing_values on CDM table we must define the name of the pipeline extension. See add_view for more information.
#' @param view_extension When using views when applying the clean_missing_values on CDM table we must define the name of the view. See add_view for more information.
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
clean_missing_values <- function(con, list_columns_clean, 
                                 to_view = FALSE, 
                                 pipeline_extension = '_T2DMM', 
                                 view_extension = '_T2DMM') {
  
  # Get available tables
  tables_available <- DBI::dbListTables(con)
  
  for (table_to_clean in names(list_columns_clean)) {
    if (table_to_clean %in% tables_available) {
      message(sprintf("[clean_missing_values] %s ", table_to_clean))
      
      # Get column types from schema
      col_info <- DBI::dbGetQuery(con, sprintf("PRAGMA table_info(%s)", table_to_clean))
      
      for (colname in list_columns_clean[[table_to_clean]]) {
        col_type <- col_info$type[col_info$name == colname]
        
        # Build filter condition based on type
        if (grepl("INT|REAL|DOUBLE|DECIMAL", toupper(col_type))) {
          filter_sql <- sprintf("%s IS NOT NULL", colname)
        } else {
          filter_sql <- sprintf("%s IS NOT NULL AND %s <> '' AND %s <> 'NA'", colname, colname, colname)
        }
        
        if (to_view == TRUE) {
          # ---- Dynamic pipeline of views ----
          transform_sql <- sprintf("SELECT * FROM %%s WHERE %s", filter_sql)
          pipeline_name <- paste0(table_to_clean, pipeline_extension)
          final_alias   <- paste0(table_to_clean, view_extension)
          
          add_view(
            con = con,
            pipeline = pipeline_name,
            base_table = table_to_clean,   # only used if pipeline not initialized
            transform_sql = transform_sql,
            final_alias = final_alias
          )
          
        } else if (to_view == FALSE) {
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
          
          message(sprintf("  -> Table %s overwritten with cleaned version", table_to_clean))
        }
      }
    } else {
      message(sprintf("Table %s does not exist", table_to_clean))
    }
  }
}
