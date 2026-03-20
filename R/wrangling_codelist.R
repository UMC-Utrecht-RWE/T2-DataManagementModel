#' Wrangle Codelist into Concept Mapping Structure
#'
#' @description
#' Transforms a study codelist by melting the `code` and `coding_system` columns
#' into separate rows in long format. This creates a standardized structure for
#' DAP-specific concept mapping.
#'
#' The output structure matches that of `wrangling_dapspecificconceptmap()`,
#' enabling seamless integration with existing concept mapping pipelines.
#'
#' @param codelist A data.table containing the study codelist with the following structure:
#'   **Required columns:**
#'   - `concept_id` (character): Harmonized concept identifier
#'   - `cdm_name` (character): CDM (Common Data Model) name/version
#'   - `cdm_table_name` (character): Name of the CDM table where the mapping applies
#'   - `code` (character): Expected code value for matching
#'   - `coding_system` (character): Coding system identifier (e.g., "ICD10", "ATC")
#'   - `keep_value_column_name` (character or NA): Column to retain as the value (can be NA)
#'   - `keep_date_column_name` (character): Column to retain as the date (cannot be NA)
#'
#' @param code_column_name Character; The CDM column name to use instead of "code" 
#'   when melting the `code` column. Default: "code"
#'
#' @param codingsystem_column_name Character; The CDM column name to use instead of "coding_system"
#'   when melting the `coding_system` column. Default: "coding_system"
#'
#' @param id_set_col Character; Name of the column to use as the unique identifier for each
#'   mapping set. If provided, the column must be integer type. If NULL, a `id_set` column 
#'   will be auto-generated as integers (1, 2, 3, ...). Default: NULL (auto-generate)
#'
#' @return
#' A data.table in long format with the following columns:
#'   \describe{
#'     \item{id_set}{Unique identifier for each mapping set (integer).}
#'     \item{concept_id}{Harmonized concept identifier (character).}
#'     \item{cdm_table_name}{CDM table name (original) (character).}
#'     \item{cdm_column}{Name of the CDM column being mapped (character).}
#'     \item{code}{Expected code value or coding system identifier (character).}
#'     \item{keep_value_column_name}{Column to retain as the value (character, may be NA).}
#'     \item{keep_date_column_name}{Column to retain as the date (character).}
#'     \item{order_index}{Order index for hierarchical application of mappings (integer, sequential within id_set).}
#'   }
#'
#'   All character columns are returned as character type (not factor).
#'   `id_set` and `order_index` are returned as integer type.
#'   Each input row produces two output rows: one for the code column and one for the coding_system column.
#'   The `order_index` is generated sequentially within each `id_set` (1, 2, 1, 2, ... for multiple id_sets).
#'   Rows are ordered by `id_set`, `concept_id`, and `cdm_column` for consistent output.
#'
#' @details
#' **Processing Steps:**
#'
#' 1. **Input Validation**: Checks for required columns and data types
#' 2. **Convert to Character**: Converts all character columns from factor to character type
#' 3. **ID Set Validation**: Validates that id_set_col (if provided) is integer type
#' 4. **NA Validation**: Ensures `keep_date_column_name` is not NA; allows `keep_value_column_name` to be NA
#' 5. **ID Set Creation**: Generates unique identifiers for each mapping set if not provided
#' 6. **Melting**: Transforms `code` and `coding_system` columns into separate rows
#' 7. **Column Name Customization**: Uses provided custom column names in `cdm_column` output
#' 8. **Filtering**: Removes entries where `code` is NA or empty string
#' 9. **Order Index Generation**: Generates sequential order_index within each id_set (resets at each new id_set)
#' 10. **Sorting & Ordering**: Orders by id_set, concept_id for consistent output
#'
#' **Example Transformation:**
#'
#' **Input (Two Rows with custom column names):**
#' ```
#' id_set  concept_id    code     coding_system  keep_value_column_name  keep_date_column_name
#' 1       WEIGHT_BIRTH  E66      ICD10          mo_source_value         mo_date
#' 2       HEIGHT_BIRTH  E67      ICD10          NA                      mo_date
#' ```
#'
#' **Output with code_column_name="actual_code", codingsystem_column_name="system":**
#' ```
#' id_set  concept_id    cdm_column       code   order_index
#' 1       WEIGHT_BIRTH  actual_code      E66    1
#' 1       WEIGHT_BIRTH  system           ICD10  2
#' 2       HEIGHT_BIRTH  actual_code      E67    1
#' 2       HEIGHT_BIRTH  system           ICD10  2
#' ```
#'
#' @examples
#' \dontrun{
#' # Example 1: Default column names (auto-generated id_set)
#' concept_codelist <- data.table(
#'   concept_id = "WEIGHT_BIRTH",
#'   cdm_name = "CONCEPTION",
#'   cdm_table_name = "MEDICAL_OBSERVATIONS",
#'   code = "E66",
#'   coding_system = "ICD10",
#'   keep_value_column_name = "mo_source_value",
#'   keep_date_column_name = "mo_date"
#' )
#'
#' wrangled <- wrangling_codelist(
#'   codelist = concept_codelist
#' )
#'
#' # Example 2: Custom id_set column (must be integer)
#' concept_codelist_with_id <- data.table(
#'   mapping_id = c(1L, 2L),
#'   concept_id = c("WEIGHT_BIRTH", "HEIGHT_BIRTH"),
#'   cdm_name = "CONCEPTION",
#'   cdm_table_name = "MEDICAL_OBSERVATIONS",
#'   code = c("E66", "E67"),
#'   coding_system = "ICD10",
#'   keep_value_column_name = "mo_source_value",
#'   keep_date_column_name = "mo_date"
#' )
#'
#' wrangled_with_id <- wrangling_codelist(
#'   codelist = concept_codelist_with_id,
#'   id_set_col = "mapping_id"
#' )
#'
#' # Example 3: Custom column names
#' wrangled_custom <- wrangling_codelist(
#'   codelist = concept_codelist,
#'   code_column_name = "actual_code",
#'   codingsystem_column_name = "coding_system_type"
#' )
#' }
#'
#' @export
wrangling_codelist <- function(
    codelist,
    code_column_name = "code",
    codingsystem_column_name = "coding_system",
    id_set_col = NULL
) {
  
  # 1. INPUT VALIDATION
  if (missing(codelist) || is.null(codelist)) {
    stop("codelist is required and cannot be NULL or missing")
  }
  
  # Ensure it's a data.table
  codelist <- ensure_data_table(codelist)
  
  if (nrow(codelist) == 0) {
    stop("codelist cannot be empty")
  }
  
  # Check for required columns
  required_cols <- c(
    "concept_id", "cdm_name", "cdm_table_name", "code", "coding_system",
    "keep_value_column_name", "keep_date_column_name"
  )
  missing_cols <- setdiff(required_cols, names(codelist))
  
  if (length(missing_cols) > 0) {
    stop(paste(
      "codelist is missing required columns:",
      paste(missing_cols, collapse = ", ")
    ))
  }
  
  # Validate data types for required columns (allow both character and factor)
  validate_column_type <- function(col_name, allowed_types = c("character")) {
    col <- codelist[[col_name]]
    is_valid <- any(sapply(allowed_types, function(type) {
      switch(type,
             "character" = is.character(col),
             "factor" = is.factor(col),
             FALSE)
    }))
    if (!is_valid) {
      stop(paste(col_name, "must be", paste(allowed_types, collapse = " or ")))
    }
  }
  
  character_cols <- c( "concept_id", "cdm_name", "cdm_table_name", "code", 
                       "coding_system", "keep_date_column_name"
  )
  for (col in character_cols) {
    validate_column_type(col, c("character"))
  }
  
  # 2. CONVERT FACTOR COLUMNS TO CHARACTER
  # Convert all character/factor columns to character type
  for (col in required_cols) {
    if (is.factor(codelist[[col]])) {
      codelist[, (col) := as.character(get(col))]
    }
  }
  
  # Validate custom column name parameters
  if (!is.character(code_column_name) || length(code_column_name) != 1) {
    stop("code_column_name must be a single character value")
  }
  
  if (!is.character(codingsystem_column_name) || length(codingsystem_column_name) != 1) {
    stop("codingsystem_column_name must be a single character value")
  }
  
  # 3. ID SET VALIDATION - must be integer if provided
  if (!is.null(id_set_col)) {
    if (!id_set_col %in% names(codelist)) {
      stop(paste("id_set_col '", id_set_col, "' not found in codelist", sep = ""))
    }
    
    if (!is.integer(codelist[[id_set_col]])) {
      stop(paste("id_set_col '", id_set_col, "' must be integer type", sep = ""))
    }
    
    setnames(codelist, id_set_col, "id_set")
  } else {
    # Auto-generate id_set as integers
    codelist[, id_set := seq_len(.N)]
  }
  
  # 4. NA VALUE VALIDATION
  # keep_date_column_name cannot be NA
  if (any(is.na(codelist$keep_date_column_name))) {
    stop("keep_date_column_name cannot contain NA values")
  }
  
  # keep_value_column_name can be NA (no validation needed)
  
  # 5. MELT CODE AND CODING_SYSTEM COLUMNS
  long_data <- melt(
    codelist,
    id.vars = c("id_set", "concept_id", "cdm_name", "cdm_table_name", 
                "keep_value_column_name", "keep_date_column_name"),
    measure.vars = c("code", "coding_system"),
    variable.name = "cdm_column",
    value.name = "code"
  )
  
  # Ensure melted columns are character type
  long_data[, cdm_column := as.character(cdm_column)]
  long_data[, code := as.character(code)]
  
  # 6. APPLY CUSTOM COLUMN NAMES
  # Replace "code" with code_column_name and "coding_system" with codingsystem_column_name
  long_data[cdm_column == "code", cdm_column := code_column_name]
  long_data[cdm_column == "coding_system", cdm_column := codingsystem_column_name]
  
  # 7. FILTER OUT NA ENTRIES
  # Remove rows where code is NA or empty string (keep_value_column_name can be NA)
  long_data <- long_data[
    !is.na(code) & code != ""
  ]
  
  # 8. SORT FOR CONSISTENT OUTPUT (BEFORE adding order_index)
  setorder(long_data, id_set, concept_id, cdm_column)
  
  # 9. ADD ORDER INDEX (after sorting, so it's sequential within each id_set)
  long_data[, order_index := seq_len(.N), by = c("id_set","concept_id","cdm_name")]
  
  # 10. FINALIZE COLUMN ORDER
  output_cols <- c(
    "id_set",
    "concept_id",
    "cdm_table_name",
    "cdm_column",
    "code",
    "keep_value_column_name",
    "keep_date_column_name",
    "order_index"
  )
  
  # Select only columns that exist
  output_cols <- intersect(output_cols, names(long_data))
  long_data <- long_data[, ..output_cols]
  
  # 11. ENSURE ALL CHARACTER COLUMNS ARE CHARACTER TYPE (NOT FACTOR)
  for (col in names(long_data)) {
    if (is.factor(long_data[[col]])) {
      long_data[, (col) := as.character(get(col))]
    }
  }
  
  # 12. ENSURE RESULT IS DATA.TABLE
  setDT(long_data)
  
  return(long_data)
}