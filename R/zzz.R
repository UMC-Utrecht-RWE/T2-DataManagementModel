#' Package initialization hook
#'
#' Suppress R CMD check notes for data.table's non-standard evaluation
#'
#' @name zzz
#' @keywords internal
utils::globalVariables(c(
  # data.table's non-standard evaluation
  ".",      # data.table's .() syntax
  ".SD",    # Subset of Data
  ".N",     # Number of rows
  ".I",     # Row indices
  ".GRP",   # Group counter
  ".BY",     # List of by values
  # Variables from apply_codelist and basic wrangling
  "code_no_dot", "family_group", "order_index", "con", "variable", "id_set",
  
  # Variables from create_dap_specific_codelist
  "length_str", "ori_length_str", "coding_system", "code_no_dot2", "COUNT", 
  "Comment", "cdm_column",
  
  # Variables using the data.table '..' prefix
  "..cols_names", "..cols_to_select", "..columns", "..keep_date_names", 
  "..keep_value_names", "..value_names",
  
  # Database and Table specific columns
  "TABLE", "Variable", "Mandatory", "Format", "ori_table", "unique_id",
  
  # Specific joined/aliased columns
  "code.CDM_CODELIST", "code.DAP_UNIQUE_CODELIST"
  
  # Add unquoted column names here if needed:
  # "column_name",
  # "another_column"
))