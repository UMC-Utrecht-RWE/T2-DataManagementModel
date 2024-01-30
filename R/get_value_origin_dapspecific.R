#' Get values from the origin database based on DAP-specific codes
#'
#' This function retrieves values from the origin database for specific 
#' DAP-specific codes.
#'
#' @param cases_dt Data table containing information about cases.
#' @param db_connection Database connection object.
#' @param codelist Data table containing DAP-specific code information.
#'
#' @return A data table with the collected values.
#'
#' @export
#'
#' @examples
#' # Example usage:
#' # get_value_origin_database_dap_specific(cases_dt, db_connection, codelist)
#'
#' @import data.table
#' @import DBI
#'
get_value_origin_dapspecific <- function(cases_dt, db_connection, codelist) {
  # Extract unique coding systems from the DAP-specific code list
  available_coding_system <- unique(codelist[, "DAP_SPEC_ID"])
  
  # Extract unique coding systems from the cases data table
  cases_coding_system <- unique(cases_dt[, "coding_system"])
  
  # Check if there are matching coding systems
  if (any(cases_coding_system %in% unlist(available_coding_system))) {
    current_dap_specific_codelist <- codelist["DAP_SPEC_ID" %in% 
                                                cases_coding_system]
  } else {
    print(paste0("[GetValueOriginDatabase]: There are no coding systems ",
                 "matching in the DAP-specific code list"))
    empty_return <- data.table::data.table()
    return(empty_return)
  }
  
  # Write cases data table to a temporary table in the database
  DBI::dbWriteTable(db_connection, "cases_tmp", cases_dt, overwrite = TRUE, 
                    temp = TRUE)
  
  # Initialize an empty list to store updated values
  updated_values <- list()
  
  # Loop through each row in the DAP-specific code list
  for (i in seq_len(nrow(current_dap_specific_codelist))) {
    column <- current_dap_specific_codelist[[i, "keep"]]
    cdm_table <- current_dap_specific_codelist[[i, "table"]]
    current_id <- current_dap_specific_codelist[[i, "DAP_SPEC_ID"]]
    
    # Query the database to get values based on the DAP-specific code
    rs <- data.table::as.data.table(DBI::dbGetQuery(db_connection, paste0(
      "SELECT t1.Ori_ID, ", column,
      " FROM ", cdm_table, " t1
                            INNER JOIN
                            cases_tmp t2
                            ON t1.Ori_ID = t2.Ori_ID
                            WHERE t2.coding_system = '", current_id, "'"
    )))
    # Rename the column in the result set
    data.table::setnames(rs, column, "Value")
    # Combine the result set with the updated values list
    updated_values <- data.table::rbindlist(list(updated_values, rs)
                                            , use.names = TRUE)
    # Remove the result set from memory
    rm(rs)
    print(paste0("[GetValueOriginDatabase] Keep column values from ", 
                 current_id, " collected"))
  }
  
  # Remove the temporary table from the database
  DBI::dbRemoveTable(db_connection, "cases_tmp")
  
  # Return unique values
  unique_updated_values <- unique(updated_values)
  return(unique_updated_values)
}
