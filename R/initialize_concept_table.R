#' Initialize Concept Table
#'
#' This function sets up the `concept_table` within the database. Depending on the 
#' \code{type_table} argument, it either creates a persistent SQL table with constraints 
#' or a virtual view that reads from Parquet files using DuckDB's \code{read_parquet} 
#' functionality.
#'
#' @param con A database connection object (typically a DuckDB connection).
#' @param type_table Character. Either \code{"view"} or \code{"table"}. If \code{"view"}, 
#'   the function creates a view pointing to Parquet files. If \code{"table"}, 
#'   it creates an empty physical table with a unique constraint.
#' @param path_parquets Character. The directory path to the Parquet files. 
#'   Required only if \code{type_table = "view"}.
#' @param partition Logical. If \code{TRUE}, enables Hive partitioning when 
#'   reading Parquet files.
#' @param overwrite Logical. If \code{TRUE}, an existing \code{concept_table} will 
#'   be dropped before being re-initialized. Defaults to \code{FALSE}.
#' @param add_id_set Logical. If \code{TRUE} (default), includes the \code{id_set} 
#'   column in the table schema or view definition.
#'
#' @details 
#' When creating a \code{"table"}, the function enforces a unique constraint on 
#' \code{(unique_id, concept_id)} to prevent duplicate concept mappings. When 
#' creating a \code{"view"}, it leverages DuckDB's ability to scan Parquet files 
#' directly, which is useful for large-scale CDM datasets.
#'
#' @return Invisibly returns the result of the \code{dbExecute} call, or 
#'   \code{NULL} if the table already exists and \code{overwrite} is \code{FALSE}.
#' 
#' @importFrom DBI dbExecute dbExistsTable dbRemoveTable
#' @export
initialize_concept_table <- function(con, 
                                     type_table = "view", 
                                     path_parquets = NULL, 
                                     partition = TRUE, 
                                     overwrite = FALSE,
                                     add_id_set = TRUE) {
  
  # 1. INPUT VALIDATION -------------------------------------------------------
  if (missing(con) || is.null(con)) {
    stop("Argument 'con' is missing or NULL.")
  }
  
  # NEW: Check if the table/view already exists
  # dbExistsTable is usually reliable across most DBI drivers
  if (dbExistsTable(con, "concept_table") &  overwrite == FALSE) {
    message("The 'concept_table' already exists in the database. Skipping initialization.")
    return(invisible(NULL))
  }
  
  if (dbExistsTable(con, "concept_table") &  overwrite == TRUE) {
    message("The 'concept_table' already exists in the database. Dropping table.")
    dbRemoveTable(con,"concept_table")
  }
  
  type_table <- tolower(type_table)
  if (!type_table %in% c("view", "table")) {
    stop("Argument 'type_table' must be either 'view' or 'table'.")
  }
  
  if (type_table == "view") {
    if (is.null(path_parquets) || !dir.exists(path_parquets)) {
      stop("A valid 'path_parquets' must be provided for a view.")
    }
  }
  
  # 2. EXECUTION --------------------------------------------------------------
  if (type_table == "view") {
    file_pattern <- if (partition) "/**/*.parquet" else "/*.parquet"
    hive_setting <- if (partition) ", hive_partitioning = TRUE" else ""
    
    sql_query <- paste0(
      "CREATE VIEW concept_table AS
       SELECT unique_id, 
              ori_table,   
              person_id, 
              concept_id,
              UPPER(CAST(value AS VARCHAR)) AS value,  
              date
              ",if(add_id_set) ", id_set","
       FROM read_parquet('", path_parquets, file_pattern, "'", hive_setting, ")"
    )
    dbExecute(con, sql_query)
    
  } else {
    dbExecute(con, paste0("
      CREATE TABLE concept_table (
          unique_id UUID, 
          ori_table TEXT, 
          person_id TEXT, 
          concept_id TEXT, 
          value TEXT, 
          date DATE,
          ",if(add_id_set) "id_set TEXT,","
          CONSTRAINT unique_ori_concept UNIQUE (unique_id, concept_id)
      );
    "))
  }
}