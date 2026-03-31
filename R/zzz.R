#' Suppress R CMD check notes for data.table's non-standard evaluation
#' @name suppress_cmd_checks
#' @keywords internal
utils::globalVariables(c(
  # data.table's non-standard evaluation
  ".", # data.table's .() syntax
  ".SD", # Subset of Data
  ".N", # Number of rows
  ".I", # Row indices
  ".GRP", # Group counter
  ".BY", # List of by values

  # data.table functions
  "setnames",
  ":=",

  # From T2.DMM package
  "DatabaseLoader",

  # Add unquoted column names here if needed:
  "ori_table",
  "unique_id",
  "TABLE",
  "Variable",
  "Mandatory",
  "Format",
  "match_status",
  "..output_cols",
  "coding_system",
  "source_column",
  "COUNT",
  "concept_id",
  "tags",
  "length_str",
  "code_substring",
  "ori_length_str",
  "code",
  "code.codelist",
  "code.dap_codes"
))
