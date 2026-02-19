#' Apply a harmonized codelist to a CDM database
#'
#' This function applies a prepared codelist to the specified Common Data Model (CDM) database
#' connection. It processes concept mappings by grouping codes into families and applying SQL
#' queries to create harmonized concepts. Parent and child relationships in the codelist are
#' handled in hierarchical order.
#'
#' @param db_con A database connection object (e.g., a DuckDB connection) where the CDM tables
#'   and concept tables are stored.
#' @param codelist A `data.table` containing the harmonized codelist. The codelist must include
#'   the following columns:
#'   \describe{
#'     \item{cdm_table_name}{Name of the CDM table or view to which the codes apply.}
#'     \item{cdm_column}{The column in the CDM table containing the code or coding system.}
#'     \item{concept_id}{The target concept identifier.}
#'     \item{code}{The source code to match.}
#'     \item{order_index}{An integer indicating processing order (parent = 1, children > 1).}
#'     \item{keep_value_column_name}{The column to keep as value when creating concepts.}
#'     \item{keep_date_column_name}{The column to keep as date when creating concepts.}
#'   }
#' @param materialize A character string specifying how to store the results. 
#'   Options are \code{"in_parquet"} (default) to save as partitioned parquet files, 
#'   or \code{"in_database"} to insert directly into a database table.
#' @param path_parquets A character string specifying the directory path where parquet 
#'   files should be saved. Required only if \code{materialize = "in_parquet"}.
#' @param keep_id_set Logical. If \code{TRUE} (default), keeps the \code{id_set} identifier 
#'   during the harmonization process.
#'
#' @details
#' The function works in three phases for each codelist family:
#' \enumerate{
#'   \item **Parent codes** are applied first using a predefined SQL script.
#'   \item **Child codes** are applied next, ordered by `order_index`.
#'   \item **Finalization** merges processed concepts back into the concepts table or 
#'   exports them to Parquet files based on the \code{materialize} argument.
#' }
#'
#' This implementation uses external SQL scripts:
#' \itemize{
#'   \item `create_concepts_1.sql` for parent concepts.
#'   \item `create_concepts_2.sql` for child concepts.
#'   \item `create_concepts_3.sql` for final merging.
#' }
#'
#' @return No return value. The function updates the database or creates parquet files 
#'   by executing SQL queries.
#'
#' @examples
#' \dontrun{
#' # Assuming `concepts_db_conn_ref` is a valid DBI connection and `codelist_long` is prepared:
#' apply_codelist(concepts_db_conn_ref, codelist_long, materialize = "in_database")
#' }
#'
#' @import data.table DBI glue
#' @importFrom here here
#' @export
  
apply_codelist <- function(db_con, 
                           codelist,
                           materialize = "in_parquet", #in_database
                           path_parquets = NULL,
                           keep_id_set = TRUE) {
  # ---- Input checks ----
  if (!DBI::dbIsValid(db_con)) {
    stop("[apply_codelist] The provided database connection is not valid.")
  }
  
  if (!data.table::is.data.table(codelist)) {
    stop("[apply_codelist] The codelist must be a data.table.")
  }
  
  if (nrow(codelist) == 0) {
    stop("[apply_codelist] The codelist is empty. Nothing to process.")
  }
  
  if (!materialize %in% c("in_parquet", "in_database")) {
    stop("Argument 'materialize' must be either 'in_parquet' or 'in_database'.")
  }
  
  # ---- Parquet path checks ----
  if (materialize == "in_parquet") {
    
    if (is.null(path_parquets)) {
      stop("[apply_codelist] 'path_parquets' must be provided when materialize = 'in_parquet'.")
    }
    
    if (!is.character(path_parquets) || length(path_parquets) != 1L) {
      stop("[apply_codelist] 'path_parquets' must be a single character string.")
    }
    
    path_parquets <- normalizePath(path_parquets, mustWork = FALSE)
    
    if (!dir.exists(path_parquets)) {
      message("[apply_codelist] Creating parquet directory: ", path_parquets)
      dir.create(path_parquets, recursive = TRUE, showWarnings = FALSE)
    }
    
    if (!dir.exists(path_parquets)) {
      stop("[apply_codelist] Failed to create parquet directory: ", path_parquets)
    }
    
    if (file.access(path_parquets, 2) != 0) {
      stop("[apply_codelist] No write permission for parquet directory: ", path_parquets)
    }
  }
  
  if (materialize == "in_database" && !is.null(path_parquets)) {
    warning(
      "[apply_codelist] 'path_parquets' is ignored when materialize = 'in_database'.",
      call. = FALSE
    )
  }
  
  required_cols <- c(
    "cdm_table_name", "cdm_column", "concept_id", "code",
    "order_index", "keep_value_column_name", "keep_date_column_name"
  )
  missing_cols <- setdiff(required_cols, colnames(codelist))
  if (length(missing_cols) > 0) {
    stop("[apply_codelist] Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  if (!is.integer(codelist$order_index) && !is.numeric(codelist$order_index)) {
    stop("[apply_codelist] 'order_index' must be numeric/integer.")
  }
  
  if(!dbExistsTable(db_con,"concept_table") & materialize %in% "in_database"){
    initialize_concept_table(db_con, 
                             type_table = "table", 
                             path_parquets = NULL, 
                             partition = TRUE,
                             add_id_set = keep_id_set)
  }
  
  # Proceed with logic
  # Group ignoring cdm_column to capture parent-child relationships
  codelist[, family_group := .GRP, by = .(
    cdm_table_name, 
    keep_value_column_name, 
    keep_date_column_name
  )]
  
  family_groups <- unique(codelist[, .(
    family_group, 
    cdm_table_name, 
    keep_value_column_name, 
    keep_date_column_name
  )])
  
  sql_dir <- here::here("R", "sql")
  
  if(length(dbListTables(db_con)) == 0){
    stop("[apply_codelist] database empty")
  }
  
  #Checkinbg if any required table exists in the database
  if(!any(unique(codelist[, cdm_table_name]) %in% dbListTables(db_con))){
    warning("[apply_codelist] requiered tables do not exist")
    return()
  }else{for (fam_idx in seq_len(nrow(family_groups))) {
    fam_info <- family_groups[fam_idx]
    message(paste0("[apply_codelist] Processing family ", fam_idx, ": ",
                   paste(fam_info, collapse = ", ")))
    # Subset family
    family_subset <- codelist[family_group == fam_info$family_group]
    
    
    cdm_table_name <-  unique(family_subset[family_group == fam_idx, cdm_table_name])
    keep_value_column_name <-  unique(family_subset[family_group == fam_idx, keep_value_column_name])
    keep_date_column_name <-  unique(family_subset[family_group == fam_idx, keep_date_column_name])
    
    for(order_idx in seq(unique(family_subset$order_index))){
      # -------------------
      # 1 Process Parent(s)
      # -------------------
      current_codelist <- family_subset[order_index == order_idx]
      for(cdm_column in unique(current_codelist[, cdm_column])){
        if(order_idx == 1){
          message("   Applying parent scheme(s)")
          dbWriteTable(db_con, name = "codelist", value = current_codelist, TEMPORARY = TRUE, overwrite = TRUE)
          
          sql_path <- system.file("sql", "create_concepts_1.sql", package = "T2.DMM")
          create_concepts_1 <- getSQL(sql_path)
          
          query_parent <- glue(create_concepts_1)
          dbExecute(db_con, query_parent)
        }
        
        # -------------------
        # 2 Process Children
        # -------------------
        if(order_idx > 1){
          child_subset <- family_subset[order_index > 1]
          if (nrow(child_subset) > 0) {
            # Each child distinguished by cdm_column
            child_groups <- unique(child_subset[, .(
              cdm_column
            )])
            
            for (child_idx in seq_len(nrow(child_groups))) {
              child_col <- child_groups[child_idx, cdm_column]
              message(paste0("    Processing child with cdm_column=", child_col))
              
              current_child <- child_subset[cdm_column == child_col]
              setorderv(current_child, "order_index")
              
              for (child_order in unique(current_child$order_index)) {
                message(paste0("        Order index: ", child_order))
                
                current_codelist <- current_child[order_index == child_order]
                dbWriteTable(db_con, name = "codelist", value = current_codelist, TEMPORARY = TRUE, overwrite = TRUE)
                current_order_index <- order_idx
                sql_path <- system.file("sql", "create_concepts_2.sql", package = "T2.DMM")
                create_concepts_2 <- getSQL(sql_path)
                query_child <- glue(create_concepts_2)
                dbExecute(db_con, query_child)
              }
            }
          }
        }
      }
      
      
    }
    # -------------------
    # 3 Finalize Family
    # -------------------
    sql_path <- system.file("sql", "create_concepts_3.sql", package = "T2.DMM")
    create_concepts_3 <- getSQL(sql_path)
    
    if(keep_id_set){
      id_set_query <- ", 
      t2.id_set"
    }else{
      id_set_query <- ""
    }
    create_concepts_3 <- glue(create_concepts_3)
    
    if(materialize == "in_database"){
      query_final <- glue("INSERT INTO concept_table 
                        {create_concepts_3}
                        ON CONFLICT DO UPDATE SET
                        unique_id = EXCLUDED.unique_id,
                        concept_id = EXCLUDED.concept_id;")
      
    }else if(materialize == "in_parquet"){
      query_final <- glue("COPY (
                            {create_concepts_3}
                            )
                          TO '{path_parquets}' 
                          (FORMAT PARQUET, 
                          PARTITION_BY (concept_id), 
                          APPEND TRUE);")
    }
    dbExecute(db_con,query_final)
    dbExecute(db_con,"DROP TABLE identified_ids;")
    
  }
    
    if(!dbExistsTable(db_con,"concept_table") & materialize %in% "in_parquet"){
      initialize_concept_table(con, 
                               type_table = "view", 
                               path_parquets = path_parquets, 
                               partition = TRUE,
                               overwrite = TRUE,
                               add_id_set = keep_id_set)
    }
    }
  
  
  
}