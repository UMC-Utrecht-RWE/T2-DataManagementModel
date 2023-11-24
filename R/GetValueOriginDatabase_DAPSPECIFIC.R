#' Get values from the origin database based on DAP-specific codes
#'
#' This function retrieves values from the origin database for specific DAP-specific codes.
#'
#' @param cases_dt Data table containing information about cases.
#' @param dbConnection Database connection object.
#' @param codelist Data table containing DAP-specific code information.
#'
#' @return A data table with the collected values.
#'
#' @export
#'
#' @examples
#' # Example usage:
#' # GetValueOriginDatabase_DAPSPECIFIC(cases_dt, dbConnection, codelist)
#'
#' @import data.table
#' @import DBI
#'
GetValueOriginDatabase_DAPSPECIFIC <- function(cases_dt, dbConnection, codelist) {
  # Extract unique coding systems from the DAP-specific code list
  available_codingSystem <- unique(codelist[, "DAP_SPEC_ID"])

  # Extract unique coding systems from the cases data table
  cases_codingSystem <- unique(cases_dt[, "coding_system"])

  # Check if there are matching coding systems
  if (any(cases_codingSystem %in% unlist(available_codingSystem))) {
    current_DAP_SPECIFIC_CODELIST <- codelist[DAP_SPEC_ID %in% cases_codingSystem]
  } else {
    print(paste0("[GetValueOriginDatabase]: There are no coding systems matching in the DAP-specific code list"))
    return(data.table())
  }

  # Write cases data table to a temporary table in the database
  tables_cases <- dbWriteTable(dbConnection, "cases_tmp", cases_dt, overwrite = TRUE, temp = TRUE)

  # Initialize an empty list to store updated values
  updated_values <- list()

  # Loop through each row in the DAP-specific code list
  for (i in 1:nrow(current_DAP_SPECIFIC_CODELIST)) {
    column <- current_DAP_SPECIFIC_CODELIST[[i, "keep"]]
    cdm_table <- current_DAP_SPECIFIC_CODELIST[[i, "table"]]
    current_id <- current_DAP_SPECIFIC_CODELIST[[i, "DAP_SPEC_ID"]]

    # Query the database to get values based on the DAP-specific code
    rs <- as.data.table(dbGetQuery(dbConnection, paste0(
      "SELECT t1.Ori_ID, ", column,
      " FROM ", cdm_table, " t1
                            INNER JOIN
                            cases_tmp t2
                            ON t1.Ori_ID = t2.Ori_ID
                            WHERE t2.coding_system = '", current_id, "'"
    )))

    # Rename the column in the result set
    setnames(rs, column, "Value")

    # Combine the result set with the updated values list
    updated_values <- rbindlist(list(updated_values, rs), use.names = TRUE)

    # Remove the result set from memory
    rm(rs)

    print(paste0("[GetValueOriginDatabase] Keep column values from ", current_id, " collected"))
  }

  # Remove the temporary table from the database
  dbRemoveTable(dbConnection, "cases_tmp")

  # Return unique values
  return(unique(updated_values))
}
