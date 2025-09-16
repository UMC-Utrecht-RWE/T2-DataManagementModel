#' Create Unique IDs for CDM Tables
#'
#' This function generates a unique CDM ID for each data record of the specified
#' Database CDM instance. This step is suggested to be applied after using the
#' loadDb function.
#' The function modifies the specified database by creating unique IDs
#' for the specified CDM tables.
#'
#' @param db_connection Database connection object.
#' @param cdm_tables_names List of CDM tables names to be imported into the db.
#' @param extension_name String to be added to the name of the tables,
#' useful when loading different CDM instances in the same database.
#' @param id_name String that defines the name of the unique identifier.
#' By default is set as "ori_id".
#' @param separator_id String that defines the separators between the table name
#' and the ROWID number.
#' @param order_by_cols List of vector with column names which you can apply an
#'  order by. E.g list(EVENTS = c('person_id','event_code'))
#' @param schema_name Optional schema name to prepend to table and view names.
#' Default is `NULL`.
#' @param to_view Logical. If `TRUE` (default), creates a view with the unique ID column.
#' If `FALSE`, overwrites the original table.
#' @param pipeline_extension When using views when applying the clean_missing_values on CDM table we must define the name of the pipeline extension. See add_view for more information.
#' @param view_extension When using views when applying the clean_missing_values on CDM table we must define the name of the view. See add_view for more information.
#' 
#' @examples
#' \dontrun{
#' # Example usage of create_unique_id
#' db_connection <- dbConnect(duckdb::duckdb(), ":memory:")
#' cdm_tables_names <- c("PERSONS", "VISITS", "OBSERVATIONS")
#' create_unique_id(db_connection, cdm_tables_names,
#'   extension_name = "_CDM1",
#'   id_name = "CDM_ID", separator_id = "_"
#' )
#' }
#'
#' @keywords internal
create_unique_id <- function(
    db_connection,
    cdm_tables_names,
    extension_name = "",
    id_name = "ori_id",
    separator_id = "-",
    order_by_cols = list(),
    schema_name = NULL,
    to_view = FALSE,
    pipeline_extension = '_T2DMM', 
    view_extension = '_T2DMM'
) {
  # Append the extension to CDM table names
  cdm_tables_names <- paste0(cdm_tables_names, extension_name)
  
  # Retrieve the names of all tables in the database
  list_existing_tables <- DBI::dbListTables(db_connection)
  
  # Get the existing tables among the specified CDM tables
  cdm_tables_names_existing <- cdm_tables_names[
    cdm_tables_names %in% list_existing_tables
  ]
  
  # Check if tables exist in the database
  if (
    length(cdm_tables_names[!cdm_tables_names %in% list_existing_tables]) > 0
  ) {
    message(paste0(
      "[CreateUniqueIDCDM] Can not create unique IDs on the following ",
      "CDM table because they do not exist in the database "
    ))
    message(cdm_tables_names[!cdm_tables_names %in% list_existing_tables])
  }
  
  order_by_flag <- FALSE
  if (length(order_by_cols) > 0) {
    order_by_flag <- TRUE
  }
  
  # Loop through each existing CDM table
  for (table in cdm_tables_names_existing) {
    # Rename the table and create a new one with the unique identifier
    if (order_by_flag && !is.null(order_by_cols[[table]])) {
      columns_in_table <- DBI::dbListFields(db_connection, table)
      cols <- order_by_cols[[table]]
      available_order_by_cols <- columns_in_table[columns_in_table %in% cols]
      order_by <- paste0(
        " ORDER BY ", paste0(available_order_by_cols, collapse = ", ")
      )
    } else {
      order_by <- ""
    }
    
    #Adjusting the name of the table to the Scheme where this is located in the database
    if(!is.null(schema_name)){
      table_from_name <- paste0(schema_name,'.',table)
    }else{
      table_from_name <- table
    }
    
    if(to_view == TRUE){
      pipeline_name <- paste0(table_from_name, pipeline_extension)
      final_alias   <- paste0(table_from_name, view_extension)
      T2.DMM:::add_view(db_connection, 
               pipeline = pipeline_name, 
               base_table = table_from_name, 
               transform_sql = paste0(
                           "SELECT '", table, separator_id,"' || row_number() OVER () AS ", id_name, ",
                                                '", table, "' AS ori_table,
                                                row_number() OVER () AS ROWID, *
                                                FROM %s",
                           order_by
                         ), 
               final_alias = final_alias)
    }else{
      DBI::dbExecute(db_connection, paste0("CREATE OR REPLACE TABLE temporal_table AS\n                                      SELECT  '", 
                                           table, separator_id, "' || row_number() OVER () AS ", id_name, 
                                           ",\n                                      '", table, 
                                           "' AS ori_table,\n\n                                      row_number() OVER () AS ROWID, *\n                                      FROM ", 
                                           table_from_name, order_by), n = -1)
      DBI::dbExecute(db_connection, paste0("DROP TABLE ", 
                                           table_from_name), n = -1)
      DBI::dbExecute(db_connection, paste0("ALTER TABLE temporal_table RENAME TO ", 
                                           table_from_name))
    }
    
    
    message(paste0("[CreateUniqueIDCDM] Unique ID create for table: ", table_from_name))
  }
}
