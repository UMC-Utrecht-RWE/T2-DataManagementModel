################################################################################
################################ Main Function #################################
################################################################################
##' Load Source Data into a DuckDB CDM Database
#'
#' This function orchestrates the full pipeline for loading source data into a
#'  Common Data Model (CDM) structure using DuckDB. It validates inputs, sets up
#'  schemas, reads source files, creates empty CDM tables, populates them, and
#'  optionally combines intermediate Parquet views.
#'
#' @param con Connection object.
#' A DuckDB connection object.
#' @param data_model Character.
#'  The data model to use (e.g., `"conception"`).
#' @param cdm_schema Character.
#'  Path to the YAML file containing the CDM schema.
#' @param format_source_files Character.
#'  Format of the source files, either `"csv"` or `"parquet"`.
#' @param folder_path_to_source_files Character.
#'  Path to the folder containing source files.
#'  Ensure the path ends with a `'/'`.
#' @param create_db_as Character.
#'  Specify whether to create the database as `"views"` or `"tables"`.
#' @param tables_in_cdm Character vector.
#' A character vector of table names that are expected in the CDM.
#'
#' @return None.
#' The function performs operations to load data into a DuckDB database.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Validates input parameters.
#'   \item In the DuckDB connection, creates necessary schemas.
#'   \item Reads source files and creates SQL views.
#'   \item Creates empty CDM tables based on the schema definition.
#'   \item Populates CDM tables from source views.
#'   \item Optionally combines intermediate Parquet views into final views.
#'   \item Closes the database connection and performs cleanup.
#' }
#'
#' @examples
#' \dontrun{
#' load_db(
#'   con = NULL,
#'   data_model = "conception",
#'   cdm_schema = "./ConcePTION_CDM_tables_v2.2.yaml",
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   create_db_as = "tables",
#'   tables_in_cdm = c("EVENTS", "MEDICINES", "PROCEDURES")
#' )
#' }
#'
#' @export
load_db <- function(
  con = NULL,
  data_model = "CDM",
  cdm_schema = NULL,
  format_source_files = "parquet",
  folder_path_to_source_files = "",
  create_db_as = "views",
  tables_in_cdm = c()
) {
  if (is.null(create_db_as) || length(create_db_as) == 0 || !(create_db_as %in% c("views", "tables"))) { # nolint
    create_db_as <- "views"
  }
  # # Create file paths to target db and parquet files
  # file_path_to_target_db <- paste0(folder_path_to_source_files,
  #                                  "/", data_model, ".duckdb")
  parquet_path <- file.path(
    folder_path_to_source_files,
    "intermediate_parquet"
  )

  if (format_source_files %in% "csv") {
    through_parquet <- "yes"
  } else {
    through_parquet <- "no"
  }
  if (through_parquet == "yes") {
    if (!dir.exists(parquet_path)) {
      dir.create(parquet_path)
    }
  }

  # What schema are we going to put the individual views to input files into?
  schema_individual_views <- "Individual_views"

  tictoc::tic()

  # 1. Check if the input parameters are correct
  # TODO: add check for tables_in_cdm?
  cat("\033[1mStep 1: Checking if input parameters are correct...\033[0m\n")
  check_params(
    yaml_path_to_cdm_schema = cdm_schema,
    format_source_files = format_source_files,
    folder_path_to_source_files = folder_path_to_source_files,
    through_parquet = through_parquet,
    create_db_as = create_db_as
  )

  # 2. Setup the database connection and create the required schemas
  cat("\033[1mStep 2: Creating required schemas in the database ...\033[0m\n")
  create_schemas(
    schema_individual_views = schema_individual_views,
    data_model = data_model,
    con = con
  )

  # 3. Read the source files as views in DuckDB
  cat("\033[1mStep 3: Reading source files as views in DuckDB...\033[0m\n")
  files_in_input <- read_source_files_as_views(
    db_connection = con,
    data_model = data_model,
    tables_in_cdm = tables_in_cdm,
    format_source_files = format_source_files,
    folder_path_to_source_files = folder_path_to_source_files,
    schema_individual_views = schema_individual_views
  )

  # 4. Create empty CDM tables with correct schema
  cat("\033[1mStep 4: Creating empty CDM tables with correct
   schema...\033[0m\n")
  create_empty_cdm_tables(
    db_connection = con,
    data_model = data_model,
    yaml_path_to_cdm_schema = cdm_schema,
    tables_in_cdm = tables_in_cdm
  )

  # 5. Populate empty CDM tables with data from source views
  cat("\033[1mStep 5: Populating empty CDM tables with data from source
   views...\033[0m\n")
  populate_cdm_tables_from_views(
    con,
    data_model,
    schema_individual_views,
    files_in_input,
    through_parquet,
    parquet_path
  )

  # 6. If through_parquet, combine views to create DB
  if (through_parquet == "yes") {
    cat("\033[1mStep 6: Combining parquet views to create database...\033[0m\n")
    combine_parquet_views(
      con,
      data_model,
      files_in_input,
      create_db_as,
      parquet_path
    )
    # 7. Add missing tables as empty tables
    # Choose schema based on through_parquet
    # if 'no', then then empty table already exists in schema_name,
    # because we created all tables in create_empty_cdm_tables()
    # if 'yes', then we need to create the empty tables in the data_model schema
    cat("\033[1mStep 7: Adding missing tables as empty tables...\033[0m\n")
    add_missing_tables_as_empty(
      con,
      data_model,
      tables_in_cdm,
      files_in_input,
      create_db_as
    )
  }

  cat("\033[1mHooray! Script finished running!\033[0m\n")

  # Final message: where to find the final tables
  view_or_table <- ifelse(create_db_as == "views", "Views", "Tables")
  cat(paste0(
    "The final tables can be accessed in the database through: \n",
    data_model, " > ", view_or_table
  ))
}
