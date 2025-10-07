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
#' @param schema_name Optional schema name to prepend to table and view names.
#' Default is `NULL`.
#' @param to_view Logical. If `TRUE` (default),
#' creates a view with the unique ID column.
#' If `FALSE`, overwrites the original table.
#' @param pipeline_extension When using views when applying the
#' clean_missing_values on CDM table we must define the name of the pipeline
#' extension. See add_view for more information.
#'
#' @examples
#' \dontrun{
#' # Example usage of create_unique_id
#' db_connection <- dbConnect(duckdb::duckdb(), ":memory:")
#' cdm_tables_names <- c("PERSONS", "VISITS", "OBSERVATIONS")
#' create_unique_id(db_connection, cdm_tables_names,
#'   extension_name = "_CDM1"
#' )
#' }
#'
#' @keywords internal
create_unique_id <- function(
  db_connection,
  cdm_tables_names,
  extension_name = "",
  schema_name = NULL,
  to_view = FALSE,
  pipeline_extension = "_T2DMM"
) {
  if (is.null(schema_name)) {
    schema_name <- "main"
  }
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

  # Loop through each existing CDM table
  for (table in cdm_tables_names_existing) {
    #Adjusting the name of the table to the Scheme where this is located
    #  in the database
    table_from_name <- paste0(schema_name, ".", table)

    if (to_view == TRUE) {
      pipeline_name <- paste0(table, pipeline_extension)
      T2.DMM:::add_view(
        db_connection,
        pipeline = pipeline_name,
        base_table = table_from_name,
        transform_sql = paste0(
          "SELECT 
          '", table, "' AS ori_table, 
          rn AS unique_id, 
          * EXCLUDE(rn)
          FROM (SELECT *, uuid() AS rn
                FROM %s)"
        )
      )
    }else{
      DBI::dbExecute(
        db_connection,
        paste0(
          "CREATE OR REPLACE TEMP TABLE temporal_table AS
            SELECT
            '", table, "' AS ori_table, 
            rn AS unique_id, 
            * EXCLUDE(rn)
            FROM (SELECT *, uuid() AS rn
                  FROM ",table_from_name,")"
        )
      )
      DBI::dbExecute(db_connection, paste0("DROP TABLE ",
                                           table_from_name), n = -1)
      DBI::dbExecute(
        db_connection,
        paste0(
          "CREATE TABLE ", table_from_name, " AS SELECT * FROM temporal_table"
        )
      )
      DBI::dbExecute(db_connection, "DROP TABLE temporal_table")
    }

    message(
      paste0(
        "[CreateUniqueIDCDM] Unique ID create for table: ",
        table_from_name
      )
    )
  }
}
