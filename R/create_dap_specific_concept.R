#' Create DAP-Specific Concepts into Database
#'
#' This function Creates DAP-specific concepts into a database using a DAP-specifc concept map (codelist).
#' The function modifies the specified save_db by creating edited tables and Creating DAP-specific concepts.
#'
#' @param codelist A data.table containing information about tables, columns, and values for DAP-specific concepts.
#' @param data_db The database connection object.
#' @param name_attachment Attachment to the database table names.
#' @param save_db The database connection object where the edited tables and concepts will be saved.
#' @param date_col_filter An optional filter to subset data based on a specified date column.
#' @param table_name Name of the table in the codelist
#' @param column_name_prefix An optional string that defines the prefix name of the column name variable column(s) from the DAP-specific concept map
#' @param expected_value_prefix An optional string that defines the prefix name of the expected value variable column(s) from the DAP-specific concept map
#' @param add_meaning An optional boolean that defines whether the possibility to save the meaning of any CDM table -if available- in the results of the function. This is specific for the ConcePTION CDM.
#' @examples
#' \dontrun{
#' # Example usage of Create_dap_specific_concept
#' codelist <- data.table::fread("path/to/codelist.csv")
#' data_db <- dbConnect(RSQLite::SQLite(), ":memory:")
#' save_db <- dbConnect(RSQLite::SQLite(), ":memory:")
#' create_dap_specific_concept(codelist, data_db, "Attachment", save_db, date_col_filter = "20230101")
#' }
#'
#' @export

create_dap_specific_concept <- function(codelist, data_db, name_attachment, save_db, date_col_filter = NULL,
                                        table_name = 'cdm_table_name',
                                        column_name_prefix = 'column_name',
                                        expected_value_prefix = 'expected_value',
                                        add_meaning = FALSE) {
  
  if (nrow(codelist) <= 0) {
    stop("Codelist does not contain any data.")
  }
  
  
  # Get unique tables from codelist
  scheme <- unique(codelist[[table_name]])
  # Get columns and value names
  cols_names <- grep(paste0("^",column_name_prefix), names(codelist), value = TRUE)
  value_names <- grep(paste0("^",expected_value_prefix), names(codelist), value = TRUE)
  # Get columns and values
  cols <- codelist[, ..cols_names]
  values <- codelist[, ..value_names]
  
  # Preprocess all possible tables:
  # Loop through each table in scheme
  for (name in scheme) {
    # Edit table name to indicate that it's been processed
    name_edited <- paste0(name, "_EDITED")
    # Get columns that need to be converted to uppercase
    to_upper_cols <- na.omit(unique(unlist(codelist[get(table_name) %in% name, ..cols_names])))
    # Get all columns from the table
    columns_db_table <- DBI::dbListFields(data_db, name)
    # Get columns that don't need to be converted to uppercase
    rest_cols <-  na.omit(columns_db_table[!columns_db_table %in% to_upper_cols])
    # Create query to convert columns to uppercase
    to_upper_query <- paste0(paste0("UPPER(", to_upper_cols, ") AS ", to_upper_cols), collapse = ", ")
    # Create query to select all columns and that are not converted to uppercase
    select_cols_query <- paste0(paste0(rest_cols, collapse = ", "), " ,")
    # If edited table doesn't exist in save_db, create a temporary table
    if (!name_edited %in% DBI::dbListTables(save_db)) {
      rs <- DBI::dbSendStatement(save_db, paste0("CREATE TEMP TABLE ", name_edited, " AS
              SELECT ", select_cols_query, " ", to_upper_query, "
              FROM ", name_attachment, ".", name))
      DBI::dbClearResult(rs)
    } else if (all(c(rest_cols, to_upper_cols) %in% DBI::dbListFields(save_db, name_edited)) == FALSE) {
      # If the edited database exists but not all the columns have been included, you need to Create the table again
      rs <- DBI::dbSendStatement(save_db, paste0("CREATE TEMP TABLE ", name_edited, " AS
              SELECT ", select_cols_query, " ", to_upper_query, "
              FROM ", name_attachment, ".", name))
      DBI::dbClearResult(rs)
    }
  }
  
  # Loop through each row in codelist
  for (j in seq_len(nrow(codelist))) {
    # Get table name, edited table name, concept name, date column, columns, and values
    table_temp <- codelist[[j, table_name]]
    name_edited <- paste0(table_temp, "_edited")
    concept_name <- codelist[[j, "concept_id"]]
    date_col <- codelist[[j, "keep_date_column_name"]]
    codelist_id <- codelist[[j, "dap_spec_id"]]
    cols_temp <- na.omit(as.character(cols[j]))
    values_temp <- toupper(na.omit(as.character(values[j])))
    value <- codelist[[j, "keep_value_column_name"]]
    if(add_meaning){
      columns_db_table <- DBI::dbListFields(save_db, name_edited)
      meaning_column_name <- columns_db_table[stringr::str_detect(columns_db_table,'meaning')]
      if(length(meaning_column_name) > 0 ){
        meaning_clause <- paste0(', ',meaning_column_name, " AS meaning ")
      }else{
        print(paste0('[create_dap_specific_concept] Meaning not identified for: ', name_edited))
        meaning_clause <- paste0(", NULL AS meaning ")
      }
     
    }else{
      meaning_clause <- ''
    }
    if (is.null(value)) {
      value <- TRUE
    } else if (any(is.na(value))) {
      value <- TRUE
    }
    if (is.null(date_col)) {
      date_col <- "'NA'"
    } else if (any(is.na(date_col))) {
      date_col <- "'NA'"
    }
    
    
    # Create coding system name
    coding_system <- paste0("'", codelist_id, "'")
    # Create where statement for the query
    where_statement <- paste(paste(cols_temp, paste0("'", values_temp, "'"), sep = " = "), collapse = " AND ")
    if (!is.null(date_col_filter)) {
      where_statement <- paste0(where_statement, " AND ", date_col, " >= ", as.integer(date_col_filter))
    }
    
    # Insert data into the concept_table in save_db
    rs <- DBI::dbSendStatement(save_db, paste0(
      "INSERT INTO concept_table
      SELECT Ori_ID, Ori_Table, ROWID, person_id, ", coding_system, " AS code, ", coding_system, " AS coding_system, ", value,
      " AS value, '", concept_name, "' AS concept_id, ", date_col, " AS date ", meaning_clause,
      "FROM ", name_edited,
      " WHERE ", where_statement
    ))
    DBI::dbClearResult(rs)
  }
}
