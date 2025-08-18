#' Transform DAP-specific concept map into a normalized long format
#'
#' This function reshapes a DAP-specific concept map (formatted according to the
#' RWE BRIDGE metadata specification) into a normalized long format suitable for
#' downstream processing and application of codelists. It expands the mapping by
#' melting multiple `column_name_*` and `expected_value_*` columns, and assigns an
#' `id_set` identifier to each original mapping entry.
#'
#' @param dap_specific_concept_map A `data.table` formatted according to the
#'   **RWE BRIDGE metadata scheme**, including at least the following columns:
#'   \describe{
#'     \item{concept_id}{The harmonized concept identifier.}
#'     \item{cdm_table_name}{Name of the CDM table where the mapping applies.}
#'     \item{keep_value_column_name}{Column name whose value should be kept when applying the mapping.}
#'     \item{keep_date_column_name}{Column name containing the date associated with the record.}
#'     \item{column_name_*}{One or more columns specifying CDM columns to map.}
#'     \item{expected_value_*}{One or more columns specifying the expected code values corresponding to the CDM columns.}
#'   }
#'
#' @details
#' This function:
#' \enumerate{
#'   \item Adds a unique identifier column `id_set` for tracking each mapping set.
#'   \item Dynamically identifies all `column_name_*` and `expected_value_*` columns.
#'   \item Converts the mapping into a long format, where each row corresponds to
#'         a specific combination of CDM column and expected value.
#'   \item Assigns an `order_index` based on the original column order.
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
#' # Assuming dap_specific_concept_map follows the RWE BRIDGE format:
#' long_map <- wrangling_dapspecificconceptmap(dap_specific_concept_map)
#' }
#'
#' @import data.table
#' @export
#' 
wrangling_dapspecificconceptmap <- function(dap_specific_concept_map){
  # ---- Input checks ----
  if (!data.table::is.data.table(dap_specific_concept_map)) {
    stop("[wrangling_dapspecificconceptmap] Input must be a data.table.")
  }
  
  if (nrow(dap_specific_concept_map) == 0) {
    stop("[wrangling_dapspecificconceptmap] Input data.table is empty.")
  }
  
  required_cols <- c("concept_id", "cdm_table_name", "keep_value_column_name", "keep_date_column_name")
  missing_cols <- setdiff(required_cols, colnames(dap_specific_concept_map))
  if (length(missing_cols) > 0) {
    stop("[wrangling_dapspecificconceptmap] Missing required columns: ", paste(missing_cols, collapse = ", "))
  }
  
  # Check for at least one column_name_* and expected_value_*
  column_names <- grep("^column_name_", names(dap_specific_concept_map), value = TRUE)
  expected_values <- grep("^expected_value_", names(dap_specific_concept_map), value = TRUE)
  
  if (length(column_names) == 0) {
    stop("[wrangling_dapspecificconceptmap] No 'column_name_*' columns found.")
  }
  if (length(expected_values) == 0) {
    stop("[wrangling_dapspecificconceptmap] No 'expected_value_*' columns found.")
  }
  # Add an ID column
  dap_specific_concept_map[, id_set := .I]
  
  # Identify column names dynamically
  column_names <- names(dap_specific_concept_map)[grep("^column_name_", names(dap_specific_concept_map))]
  expected_values <- names(dap_specific_concept_map)[grep("^expected_value_", names(dap_specific_concept_map))]
  
  # Initialize an empty list to store results
  results_list <- list()
  
  # Loop through each column_name and melt
  for (index in seq(1,length(column_names))) {
    melted <- melt(
      dap_specific_concept_map,
      id.vars = c("id_set","concept_id","cdm_table_name","keep_value_column_name","keep_date_column_name", column_names[index]),       # Use the current column_name
      measure.vars = expected_values[index], value.name = 'code'       # Use all expected_value columns
    )
    setnames(melted,column_names[index],'cdm_column')
    melted[, variable := NULL][, order_index := index]
    results_list <- rbindlist(list(results_list,melted), use.names = TRUE, fill = TRUE)
  }
  results_list <- results_list[!is.na(cdm_column)]
  #results_list_casted <- dcast(results_list, id_set + cdm_table_name ~ column ,value.var = 'value')
  return(results_list)
}