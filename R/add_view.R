#' Add a transformation step to a DuckDB view pipeline
#'
#' This function adds a new transformation step to a versioned view pipeline stored entirely
#' within DuckDB. Each step creates a new view based on the previous step, and a final alias
#' view (e.g., `persons_view`) always points to the latest version of the pipeline.
#'
#' If the pipeline does not exist yet, the function will automatically initialize it from
#' a specified base table and create the first version.
#'
#' @param con A DBI connection object to a DuckDB database.
#' @param pipeline A string specifying the name of the pipeline.
#' @param transform_sql A SQL string representing the transformation. Use `%s` as a placeholder
#'   for the current head view of the pipeline.
#' @param base_table Optional. The name of the base table to initialize the pipeline if it
#'   does not exist. Required only for the first step.
#' @param final_alias The name of the final view that should always point to the latest
#'   version of the pipeline.
#'
#' @return Invisibly returns the name of the newly created versioned view in the pipeline.
#'
#' @examples
#' # Initialize or add to the pipeline
#' add_view(con, "persons_pipeline",
#'          "SELECT DISTINCT * FROM %s",
#'          base_table = "persons",
#'          final_alias = "persons_view")
#'
#' add_view(con, "persons_pipeline",
#'          "SELECT *, LENGTH(name) AS name_len FROM %s",
#'          final_alias = "persons_view")
#'
#' # Query the final transformed view
#' DBI::dbGetQuery(con, "SELECT * FROM persons_view LIMIT 5")
#' @keywords internal
#' 
add_view <- function(con, pipeline, transform_sql, base_table = NULL, final_alias) {
  # Ensure the pipeline registry exists
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS _pipeline_registry (
      pipeline_name TEXT PRIMARY KEY,
      current_view TEXT
    )
  ")
  # Check if the pipeline exists
  exists <- DBI::dbGetQuery(con, sprintf("
    SELECT COUNT(*) AS n FROM _pipeline_registry WHERE pipeline_name = '%s'
  ", pipeline))$n > 0
  
  if (!exists) {
    if (is.null(base_table)) {
      stop("Pipeline does not exist yet. Please provide a base_table to initialize it.")
    }
    # Initialize pipeline
    first_view <- paste0(pipeline, "_v1")
    
    DBI::dbExecute(con, sprintf(
      "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", 
      first_view, base_table
    ))
    
    DBI::dbExecute(con, sprintf("
      INSERT INTO _pipeline_registry (pipeline_name, current_view)
      VALUES ('%s', '%s')
    ", pipeline, first_view))
    
    # Create final alias pointing to first view
    DBI::dbExecute(con, sprintf(
      "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
      final_alias, first_view
    ))
  }
  
  # Get current head
  current_view <- DBI::dbGetQuery(con, sprintf("
    SELECT current_view FROM _pipeline_registry WHERE pipeline_name = '%s'
  ", pipeline))$current_view
  
  # New version
  version <- as.integer(sub(".*_v", "", current_view)) + 1
  new_view <- paste0(pipeline, "_v", version)
  
  # Apply transformation
  DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE VIEW %s AS %s",
    new_view, sprintf(transform_sql, current_view)
  ))
  
  # Update registry
  DBI::dbExecute(con, sprintf("
    UPDATE _pipeline_registry SET current_view = '%s' WHERE pipeline_name = '%s'
  ", new_view, pipeline))
  
  # Update final alias
  DBI::dbExecute(con, sprintf(
    "CREATE OR REPLACE VIEW %s AS SELECT * FROM %s",
    final_alias, new_view
  ))
  
  invisible(new_view)
}
