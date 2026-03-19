#' Transform DAP-specific concept map into a normalized long format
#'
#' This function reshapes a codelist (formatted according to the
#' RWE BRIDGE metadata specification) into a normalized long format suitable for
#' downstream processing and application of codelists.
#'
#' @param codelist A `data.table` formatted according to the
#'   **RWE BRIDGE metadata scheme**, including at least the following columns:
#'   \describe{
#'     \item{concept_id}{The harmonized concept identifier.}
#'     \item{cdm_name}{}
#'     \item{cdm_table_name}{Name of the CDM table where the mapping applies.}
#'     \item{code}{
#'      }
#'     \item{coding_system}{
#'        
#'      }
#'   }
#'
#' @details
#' This function:
#' \enumerate{
#'   \item Adds a unique identifier column `id_set`
#'      for tracking each mapping set.
#'   
#' }
#'
#' The resulting structure supports hierarchical processing of concept mappings
#' when applying them to a CDM database.
#'
#' @return A `data.table` in long format with the following columns:
#'   \describe{
#'     \item{id_set}{Unique identifier for each mapping set.}
#'     \item{concept_id}{Harmonized concept identifier.}
#'     \item{cdm_table_name}{CDM table name (original).}
#'     \item{cdm_column}{Name of the CDM column being mapped.}
#'     \item{code}{Expected code for the CDM column.}
#'     \item{keep_value_column_name}{Column to retain as the value.}
#'     \item{keep_date_column_name}{Column to retain as the date.}
#'     \item{order_index}{Order index for hierarchical application of mappings.}
#'   }
#'
#' @examples
#' \dontrun{
#' # Assuming codelist follows the RWE BRIDGE format:
#' long_map <- wrangling_codelist(codelist)
#' }
#'
#' @import data.table
#' @export
#'
wrangling_dapspecificcodelist <- function(codelist, keep_date_column_name = NULL, keep_value_column_name = NULL) {
  # ---- Input checks ----
  if (!data.table::is.data.table(codelist)) {
    stop("[wrangling_codelist] Input must be a data.table.")
  }
  
  if (nrow(codelist) == 0) {
    stop("[wrangling_codelist] Input data.table is empty.")
  }
  
  output_cols <- c(
    "coding_system", "COUNT", "source_column",
    "code.dap_codes", "concept_id", "code.codelist",
    "tags", "length_str", "code", "match_status"
  )
  
  missing_cols <- setdiff(required_cols, colnames(codelist))
  if (length(missing_cols) > 0) {
    stop(
      "[wrangling_codelist] Missing required columns: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  
  # Add an ID column
  codelist[, id_set := .I]
  
  
  # Initialize an empty list to store results
  results_list <- list()
  
  # Loop through each column_name and melt
  for (index in seq(1, length(column_names))) {
    melted <- melt(
      codelist,
      id.vars = c(
        "id_set", "concept_id", "cdm_table_name", "keep_value_column_name",
        "keep_date_column_name", column_names[index]
      ),  # Use the current column_name
      measure.vars = expected_values[index],
      value.name = "code" # Use all expected_value columns
    )
    setnames(melted, column_names[index], "cdm_column")
    melted[, variable := NULL][, order_index := index]
    results_list <- rbindlist(
      list(results_list, melted), use.names = TRUE, fill = TRUE
    )
  }
  results_list[!is.na(cdm_column)]
}
