#' Initialize Concept Table
#'
#' @param con A database connection object.
#' @param type_table Character. Either `"view"` or `"table"`.
#' @param path_parquets Character. Path to Parquet files.
#' @param partition Logical. If `TRUE`, uses Hive partitioning.
#'
#' @return Invisibly returns the result of the `dbExecute` call or NULL if skipped.
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