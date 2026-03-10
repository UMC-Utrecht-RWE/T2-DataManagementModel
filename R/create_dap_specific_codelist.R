#' Create DAP-Specific Codelist
#'
#' @param dap_codes Data.table containing unique code list output.
#' @param codelist Data.table containing the study code list.
#' @param start_with_codingsystems Columns to start with.
#' @param priority_col Name of the priority column to use for sorting. Defaults to "priority".
#'
#' @export
create_dap_specific_codelist <- function(
    dap_codes,
    codelist,
    start_with_codingsystems = c(
      "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
      "ICPC", "ICPC2P", "ICPC2EENG", "ATC"),
    priority_col = "priority"
) {
  
  # 1. PARAMETER VALIDATION
  if (!is.character(start_with_codingsystems)) {
    stop("start_with_codingsystems must be a character vector")
  }
  message(paste0("-----> The following coding systems will be searched 
                 with a start with approach: ",
          paste0(start_with_codingsystems, collapse = ", ")))
  
  # 2. INPUT VALIDATION
  # Ensure data.tables are not empty and contain mandatory columns
  validate_codelists(dap_codes, codelist, priority_col)
  
  # 3. COLUMN STANDARDIZATION
  # Distinguish the source of the 'code' column before merging to prevent collisions
  codelist[, code.codelist := code]
  dap_codes[, code.dap_codes := code]

  # Calculate string length for study codes to determine the minimum prefix needed for matching
  codelist[, length_str := nchar(code.codelist)] 
  min_length_codelist <- min(codelist$length_str, na.rm = TRUE)
  
  # 4. DATA SPLITTING
  # Identify rows belonging to coding systems that allow prefix matching (e.g., ATC, ICD10)
  is_start_with_dapcodes <- dap_codes$coding_system %in% start_with_codingsystems
  is_start_with_codelist <- codelist$coding_system %in% start_with_codingsystems
  
  # Split datasets into groups requiring exact matches vs hierarchical start-with matches
  start_dap_codes <- dap_codes[is_start_with_dapcodes]
  start_codelist <- codelist[is_start_with_codelist]
  exact_dap_codes <- dap_codes[!is_start_with_dapcodes]
  exact_codelist <- codelist[!is_start_with_codelist]
  
  # 5. EXACT MATCHING LOGIC
  # Performs a direct join on coding system and the raw code string for systems like 'PRODCODEID'
  exact_match <- data.table()
  if (nrow(exact_dap_codes) > 0 && nrow(exact_codelist) > 0) {
    exact_match <- data.table::merge.data.table(
      exact_dap_codes, exact_codelist,
      by.x =  c("coding_system", "code"),
      by.y =  c("coding_system", "code"), 
    )
  }
  
  # 6. START-WITH (HIERARCHICAL) MATCHING LOGIC
  results_startwith <- data.table()
  if (nrow(start_dap_codes) > 0 && nrow(start_codelist) > 0) {
    
    # Calculate lengths of the actual codes found in the database (DAP)
    start_dap_codes[, ori_length_str := nchar(code.dap_codes)]
    max_code_length <- max(start_dap_codes$ori_length_str)
    message(paste0("[SetCodesheets] Max length of 
                   code from the DAP is : ", max_code_length))
    
    # Define the range of possible prefix lengths to search (Study min length to DAP max length)
    length_range <- seq(min_length_codelist, max_code_length)
    
    # SUBSTRING EXPANSION:
    # Iterate through all possible lengths. For each DAP code, generate its parent prefixes.
    # e.g., DAP code 'N02BE01' will generate rows for 'N02', 'N02B', 'N02BE', etc.
    start_expanded <- rbindlist(lapply(length_range, function(len) {
      temp_dt <- start_dap_codes[ori_length_str >= len]
      if (nrow(temp_dt) > 0) {
        temp_dt[, `:=`(
          code_substring = substr(code.dap_codes, 1, len),
          length_str = len
        )]
        return(temp_dt[, .(coding_system, code.dap_codes, 
                           code_substring, length_str, 
                           ori_length_str, COUNT, source_column)])
      }
      data.table()
    }))
    
    if (nrow(start_expanded) > 0) {
      # Merge the expanded DAP prefixes against the study codelist codes
      results_startwith <- data.table::merge.data.table(
        start_expanded,
        start_codelist,
        by.x = c("coding_system", "code_substring", "length_str"),
        by.y = c("coding_system", "code.codelist", "length_str"),
        allow.cartesian = TRUE
      )
      
      # TIE-BREAKING & DEDUPLICATION:
      # If one granular DAP code matches multiple study codes (e.g., both 'N02' and 'N02B'),
      # we prioritize the longest match (most specific) and then the priority column.
      if (nrow(results_startwith) > 0) {
        cols_by <- c("code.dap_codes", "concept_id", "coding_system")
        
        # Sort by specificity (Length Descending) then Priority (Ascending)
        setorderv(results_startwith, c("length_str", priority_col), c(-1, 1))
        
        # Keep only the single best match per unique DAP code
        results_startwith <- results_startwith[, .SD[1], by = cols_by]
        data.table::setnames(results_startwith,"code_substring","code.codelist")
      }
    }
  }

  # 7. COMBINE AND LABEL RESULTS
  
  # Define the expected columns for the match table to prevent errors if empty
  match_cols <- unique(c(
    names(exact_dap_codes), names(exact_codelist), 
    "code.dap_codes", "code.codelist", "length_str"
  ))
  
  # Initialize as an empty data.table with the correct columns
  all_matches <- data.table(matrix(ncol = length(match_cols), nrow = 0))
  setnames(all_matches, match_cols)
  
  # Combine results (rbindlist handles the types if results exist)
  found_matches <- rbindlist(list(exact_match, results_startwith), use.names = TRUE, fill = TRUE)
  
  if (nrow(found_matches) > 0) {
    all_matches <- rbindlist(list(all_matches, found_matches), fill = TRUE)
  }
  
  # Identify records that failed to match using anti-joins
  # These will now work even if all_matches is empty because the columns exist
  missing_from_cdm <- dap_codes[!all_matches, on = .(coding_system, code.dap_codes)]
  missing_from_codelist <- codelist[!all_matches, on = .(coding_system, code.codelist)]
  
  # Final vertical stack: Matches, Codes only in DAP, and Codes only in Study Codelist
  dap_specific_codelist <- rbindlist(
    list(all_matches, missing_from_cdm, missing_from_codelist),
    fill = TRUE
  )
  
  dap_specific_codelist[,code := fifelse(!is.na(code.dap_codes) & !is.na(code.codelist),
                                        code.dap_codes,
                                        NA)]
  
  # 8. match_statusING LOGIC
  # Label the source/status of each entry
  dap_specific_codelist[, match_status := fcase(
    is.na(code.dap_codes), "ONLY_IN_CODELIST", # Present in study list, absent in database (DAP)
    is.na(code.codelist), "ONLY_IN_DATA",       # Present in database (DAP), absent in study list
    default = "MATCHED"                         # Successful match identified
  )]
  # Final column cleanup and selection for output
  output_cols <- c(
    "coding_system", "COUNT", "source_column",
    "code.dap_codes", "concept_id", "code.codelist",
    "tags", priority_col, "length_str", "code", "match_status"
  )
  
  missing_cols <- output_cols[output_cols %notin% names(dap_specific_codelist)]
  lapply(missing_cols, 
         function (x) dap_specific_codelist[, eval(x) := NA])
  
  return(dap_specific_codelist[, ..output_cols])
}


# Data Input Requirements Validation
validate_codelists <- function(dap_codes, codelist, priority_col) {
  # Check if inputs are provided
  if (missing(dap_codes) || is.null(dap_codes)) {
    stop("dap_codes is required and cannot be NULL or missing")
  }
  if (missing(codelist) || is.null(codelist)) {
    stop("codelist is required and cannot be NULL or missing")
  }

  # Ensure entries are data.table
  dap_codes <- ensure_data_table(dap_codes)
  codelist <- ensure_data_table(codelist)
  
  # Check if data.tables are not empty
  if (nrow(dap_codes) == 0) stop("dap_codes cannot be empty")
  if (nrow(codelist) == 0) stop("codelist cannot be empty")

  # Required columns validation
  required_unique_cols <- c("coding_system", "code")
  required_study_cols <- c("coding_system", "code", "concept_id", priority_col)
  
  # PRIORITY HANDLING
  # Check if the user-defined priority column exists in the study data.
  # If missing, assign a default value of 1 to all rows so the sort logic still runs.
  if (!priority_col %in% names(codelist)) {
    message(paste0("Column '", priority_col, "' not found. Assigning default value 1."))
    codelist[, (priority_col) := 1]
  }
  
  missing_unique_cols <- setdiff(required_unique_cols, names(dap_codes))
  missing_study_cols <- setdiff(required_study_cols, names(codelist))
  
  if (length(missing_unique_cols) > 0) {
    stop(paste("dap_codes is missing required columns:",
               paste(missing_unique_cols, collapse = ", ")))
  }
  if (length(missing_study_cols) > 0) {
    stop(paste("codelist is missing required columns:",
               paste(missing_study_cols, collapse = ", ")))
  }

  # Validate column data types
  validate_column_type <- function(
    col, name, allowed_types = c("character", "factor")
  ) {
    if (!any(sapply(allowed_types, function(type) {
      switch(type,
             "character" = is.character(col),
             "factor" = is.factor(col))
    }))) {
      stop(paste(name, "must be character or factor"))
    }
  }

  validate_column_type(
    dap_codes$coding_system, "dap_codes$coding_system"
    )
  validate_column_type(
    codelist$coding_system, "codelist$coding_system"
    )
  validate_column_type(dap_codes$code, "dap_codes$code")
  validate_column_type(codelist$code, "codelist$code")

  # Check for NA values and warn
  na_checks <- list(
    list(
      dap_codes$coding_system,
      "dap_codes contains NA values in coding_system column"
    ),
    list(
      codelist$coding_system,
      "codelist contains NA values in coding_system column"
    ),
    list(
      dap_codes$code,
      "dap_codes contains NA values in code column"
    ),
    list(
      codelist$code,
      "codelist contains NA values in code column"
    ),
    list(
      codelist$priority,
      "codelist contains NA values in priority column"
    )
  )

  invisible(lapply(na_checks, function(check) {
    if (any(is.na(check[[1]]))) warning(check[[2]])
  }))

}
