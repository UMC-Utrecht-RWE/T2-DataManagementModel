#' Create DAP-Specific Codelist
#'
#' This function merges the unique codelist from a database
#' (you can use the function getUniqueCodeList) with a study code list.
#'
#' @param unique_codelist Data.table containing unique code list output
#' from the function getUniqueCodeList. Must contain columns: 'coding_system',
#' 'code', 'concept_id', and 'code.unique_condelist'.
#' @param study_codelist Data.table containing the study code list.
#' Must contain columns: 'coding_system' and 'code'.
#' @param start_with_colls Columns to start with
#' @param priority Priority column for selecting codes when there are
#' multiple matches.
#'
#' @return A merged data.table containing DAP-specific codes,
#' including exact matches and those starting with.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' result <- create_dap_specific_codelist(unique_codelist, study_codelist,
#'   priority = NA
#' )
#' }
#'
#' @export
create_dap_specific_codelist <- function(
    unique_codelist,
    study_codelist,
    start_with_colls = c(
      "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
      "ICPC", "ICPC2P", "ICPC2EENG", "ATC", "vx_atc"
    ),
    priority = NA) {
  
  # Validate start_with_colls parameter
  if (!is.character(start_with_colls)) {
    stop("start_with_colls must be a character vector")
  }
  
  # Validate inputs and get cleaned data
  validate_codelists(unique_codelist, study_codelist, priority)
  
  # Preprocess both datasets
  study_codelist <- add_codenodot(study_codelist, "code")
  data.table::setnames(study_codelist, "code.study_codelist", "code.CDM_CODELIST")
  min_length_study_codelist <- min(study_codelist$length_str, na.rm = TRUE)
  
  unique_codelist <- add_codenodot(unique_codelist, "code")
  setnames(unique_codelist, "code.unique_codelist", "code.DAP_UNIQUE_CODELIST")
  
  # Identify rows that will be use in a start with approach
  is_start_with_unique <- unique_codelist$coding_system %in% start_with_colls
  is_start_with_study <- study_codelist$coding_system %in% start_with_colls
  
  # Split datasets using logical indexing
  start_unique_codelist <- unique_codelist[is_start_with_unique]
  start_study_codelist <- study_codelist[is_start_with_study]
  exact_unique_codelist <- unique_codelist[!is_start_with_unique]
  exact_study_codelist <- study_codelist[!is_start_with_study]
  
  # Identify exact matches
  exact_match <- data.table()
  if (nrow(exact_unique_codelist) > 0 && nrow(exact_study_codelist) > 0) {
    exact_match <- merge(
      exact_unique_codelist, exact_study_codelist,
      by = c("coding_system", "code_no_dot")
    )
  }
  
  # Process start-with matches
  results_startwith2 <- data.table()
  if (nrow(start_unique_codelist) > 0 && nrow(start_study_codelist) > 0) {
    # Handle exact matches in start-with category first
    start_exact_match <- merge(
      start_unique_codelist, start_study_codelist,
      by = c("coding_system", "code_no_dot")
    )
    
    # Create length of codes
    start_unique_codelist[, ori_length_str := nchar(code_no_dot)]
    max_code_length <- max(start_unique_codelist$ori_length_str)
    message(paste0("[SetCodesheets] Max length of code from the DAP is : ", max_code_length))
    
    # Substring generation
    length_range <- seq(min_length_study_codelist, max_code_length)
    
    # Create all substring combinations at once
    start_expanded <- rbindlist(lapply(length_range, function(len) {
      temp_dt <- start_unique_codelist[ori_length_str >= len]
      if (nrow(temp_dt) > 0) {
        temp_dt[, `:=`(
          code_no_dot2 = substr(code_no_dot, 1, len),
          length_str = len
        )]
        return(temp_dt[, .(coding_system, code.DAP_UNIQUE_CODELIST, code_no_dot, 
                           code_no_dot2, length_str, ori_length_str, COUNT, variable)])
      }
      return(data.table())
    }))
    
    #Adding original information and selecting codes
    if (nrow(start_expanded) > 0) {
      results_startwith <- data.table::merge.data.table(
        start_expanded,
        start_study_codelist,
        by.x = c("coding_system", "code_no_dot2", "length_str"),
        by.y = c("coding_system", "code_no_dot", "length_str"),
        allow.cartesian = TRUE
      )
      
      # Selecting the longest code (the deepest children)
      if (nrow(results_startwith) > 0) {
        cols_by <- c("code.DAP_UNIQUE_CODELIST", "concept_id", "coding_system")
        
        if (!is.na(priority)) {
          setorderv(results_startwith, c("length_str", priority), c(-1, 1))
        } else {
          setorderv(results_startwith, "length_str", -1)
        }
        
        results_startwith2 <- results_startwith[, .SD[1], by = cols_by]
      }
    }
  }
  
  # Combine results
  all_matches <- rbindlist(list(exact_match, results_startwith2), fill = TRUE)
  
  # Find missing codes using anti-joins
  missing_from_cdm <- unique_codelist[!all_matches, 
                                      on = c("coding_system", "code_no_dot")]
  missing_from_codelist <- study_codelist[!all_matches, 
                                          on = c("coding_system", "code_no_dot")]
  
  # Final combination
  dap_specific_codelist <- rbindlist(
    list(all_matches, missing_from_cdm, missing_from_codelist),
    fill = TRUE
  )
  
  # Add Comment
  # CODELIST when no code was identified in the CDM data instance
  # CDM when the code is not identified by the codelist
  # BOTH when the code is found in both codelist and CDM
  dap_specific_codelist[, Comment := fcase(
    is.na(code.DAP_UNIQUE_CODELIST), "CODELIST",
    is.na(code.CDM_CODELIST), "CDM",
    default = "BOTH"
  )]
  
  cols_to_select <- c(
    "coding_system", 
    "code_no_dot", 
    "COUNT", 
    "variable", 
    "code.DAP_UNIQUE_CODELIST",
    "concept_id", 
    "code.CDM_CODELIST", 
    "tags", 
    "priority", 
    "length_str", 
    "Comment"
  )
  
  # Subset and keep the order
  dap_specific_codelist <- dap_specific_codelist[, ..cols_to_select]
  
  return(dap_specific_codelist)
}

# Data Input Requirements Validation
validate_codelists <- function(unique_codelist, study_codelist, priority) {
  # Check if inputs are provided
  if (missing(unique_codelist) || is.null(unique_codelist)) {
    stop("unique_codelist is required and cannot be NULL or missing")
  }
  if (missing(study_codelist) || is.null(study_codelist)) {
    stop("study_codelist is required and cannot be NULL or missing")
  }
  
  # Ensure entries are data.table
  unique_codelist <- ensure_data_table(unique_codelist)
  study_codelist <- ensure_data_table(study_codelist)
  
  # Check if data.tables are not empty
  if (nrow(unique_codelist) == 0) stop("unique_codelist cannot be empty")
  if (nrow(study_codelist) == 0) stop("study_codelist cannot be empty")
  
  # Required columns validation
  required_unique_cols <- c("coding_system", "code")
  required_study_cols <- c("coding_system", "code", "concept_id")
  
  missing_unique_cols <- setdiff(required_unique_cols, names(unique_codelist))
  missing_study_cols <- setdiff(required_study_cols, names(study_codelist))
  
  if (length(missing_unique_cols) > 0) {
    stop(paste("unique_codelist is missing required columns:",
               paste(missing_unique_cols, collapse = ", ")))
  }
  if (length(missing_study_cols) > 0) {
    stop(paste("study_codelist is missing required columns:",
               paste(missing_study_cols, collapse = ", ")))
  }
  
  # Validate column data types
  validate_column_type <- function(col, name, allowed_types = c("character", "factor")) {
    if (!any(sapply(allowed_types, function(type) {
      switch(type,
             "character" = is.character(col),
             "factor" = is.factor(col))
    }))) {
      stop(paste(name, "must be character or factor"))
    }
  }
  
  validate_column_type(unique_codelist$coding_system, "unique_codelist$coding_system")
  validate_column_type(study_codelist$coding_system, "study_codelist$coding_system")
  validate_column_type(unique_codelist$code, "unique_codelist$code")
  validate_column_type(study_codelist$code, "study_codelist$code")
  
  # Check for NA values and warn
  na_checks <- list(
    list(unique_codelist$coding_system, "unique_codelist contains NA values in coding_system column"),
    list(study_codelist$coding_system, "study_codelist contains NA values in coding_system column"),
    list(unique_codelist$code, "unique_codelist contains NA values in code column"),
    list(study_codelist$code, "study_codelist contains NA values in code column")
  )
  
  invisible(lapply(na_checks, function(check) {
    if (any(is.na(check[[1]]))) warning(check[[2]])
  }))
  
  # Validate priority parameter
  if (!is.na(priority)) {
    if (!is.character(priority) || length(priority) != 1) {
      stop("priority must be a single character string or NA")
    }
    if (!priority %in% names(study_codelist)) {
      stop(paste("priority column '", priority, "' not found in study_codelist"))
    }
  }
}

# Preprocessing - use data.table's reference semantics for efficiency
add_codenodot <- function(dt, code_col_name) {
  dt[, code_no_dot := gsub("\\.", "", get(code_col_name))]
  if (code_col_name == "code") {
    dt[, length_str := nchar(code_no_dot)]  # nchar is faster than stringr::str_length
    setnames(dt, "code", paste0("code.", deparse(substitute(dt))))
  }
  return(dt)
}