################################################################################
################################ Main Function #################################
################################################################################

library(tidyverse)

##' Load Source Data into a DuckDB CDM Database
#'
#' This function orchestrates the full pipeline for loading source data into a
#'  Common Data Model (CDM) structure using DuckDB. It validates inputs, sets up
#'  schemas, reads source files, creates empty CDM tables, populates them, and
#'  optionally combines intermediate Parquet views.
#'
#' @param data_model Character.
#'  The data model to use (e.g., `"conception"`).
#' @param excel_path_to_cdm_schema Character.
#'  Path to the Excel file containing the CDM schema.
#' @param format_source_files Character.
#'  Format of the source files, either `"csv"` or `"parquet"`.
#' @param folder_path_to_source_files Character.
#'  Path to the folder containing source files.
#'  Ensure the path ends with a `'/'`.
#' @param through_parquet Character.
#'  Whether to process through Parquet files (`"yes"` or `"no"`).
#' @param file_path_to_target_db Character.
#'  Full path to the target database file, including the `".duckdb"` extension.
#' @param create_db_as Character.
#'  Specify whether to create the database as `"views"` or `"tables"`.
#' @param verbosity Integer.
#'  Level of verbosity for logging; `0` for minimal output, `1` for detailed output.
#'
#' @return None.
#' The function performs operations to load data into a DuckDB database.
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Validates input parameters.
#'   \item Sets up the DuckDB connection and creates necessary schemas.
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
#'   data_model = "conception",
#'   excel_path_to_cdm_schema = "./ConcePTION_CDM tables v2.2.xlsx",
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   through_parquet = "no",
#'   file_path_to_target_db = "data/target/my_database.duckdb",
#'   create_db_as = "tables",
#'   verbosity = 1
#' )
#' }
#'
#' @export
load_db <- function(
    data_model = "conception",
    excel_path_to_cdm_schema = "./ConcePTION_CDM tables v2.2.xlsx",
    format_source_files = "parquet",
    folder_path_to_source_files = "",
    through_parquet = "yes",
    file_path_to_target_db = "",
    create_db_as = "views",
    verbosity = 1) {
  # Sanitize input parameters
  if (!(create_db_as %in% c("views", "tables"))) {
    create_db_as <<- "views"
  }
  if (!(verbosity %in% c(0, 1))) {
    verbosity <<- 1
  }

  # What schema are we going to put the individual views to input files into?
  schema_individual_views <- "Individual_views"
  # What schema will we put the combined views of created parquet files into?
  schema_combined_views <- "Combined_views"
  # What schema will be the target CDM with the actual tables?
  schema_conception <- "Empty_Conception_tables"

  # List of expected tables in the CDM
  if (data_model == "conception") {
    tables_in_cdm <- c(
      "CDM_SOURCE",
      "EVENTS",
      "EUROCAT",
      "INSTANCE",
      "MEDICAL_OBSERVATIONS",
      "MEDICINES",
      "METADATA",
      "OBSERVATION_PERIODS",
      "PERSON_RELATIONSHIPS",
      "PERSONS",
      "PRODUCTS",
      "PROCEDURES",
      "SURVEY_ID",
      "SURVEY_OBSERVATIONS",
      "VACCINES",
      "VISIT_OCCURRENCE"
    )
  }

  tictoc::tic()

  # 1. Check if the input parameters are correct
  check_params(data_model,
               excel_path_to_cdm_schema,
               format_source_files,
               folder_path_to_source_files,
               through_parquet,
               file_path_to_target_db,
               create_db_as,
               verbosity)
  # 2. Setup the database connection and create the required schemas
  con <- setup_db_connection(schema_individual_views,
                             schema_conception,
                             schema_combined_views,
                             file_path_to_target_db)
  # 3. Read the source files as views in DuckDB
  files_in_input <- read_source_files_as_views(con,
                                               data_model,
                                               tables_in_cdm,
                                               format_source_files,
                                               folder_path_to_source_files,
                                               schema_individual_views)
  # 4. Create empty CDM tables with correct schema
  create_empty_cdm_tables(con,
                          data_model,
                          excel_path_to_cdm_schema,
                          tables_in_cdm,
                          schema_conception)
  # 5. Populate empty CDM tables with data from source views
  populate_cdm_tables_from_views(con,
                                 data_model,
                                 schema_individual_views,
                                 schema_conception,
                                 files_in_input,
                                 through_parquet,
                                 create_db_as)
  # 6. If through_parquet, combine views to create DB
  if (through_parquet == "yes") {
    combine_parquet_views(con,
                          data_model,
                          schema_conception,
                          schema_combined_views,
                          files_in_input,
                          create_db_as)
  }
  # Close the connection
  DBI::dbDisconnect(con, shutdown = TRUE)
  rm(con)
  invisible(gc())
  tictoc::toc()
  cat("Hooray! Script finished running!\n")

  # Choose schema based on create_db_as
  schema_with_final_tables <- ifelse(create_db_as == "views",
                                     schema_combined_views,
                                     schema_conception)

  # Override if through_parquet is "no"
  if (tolower(through_parquet) == "no") {
    schema_with_final_tables <- schema_conception
  }

  view_or_table <- ifelse(create_db_as == "views", "Views", "Tables")

  # Final message: where to find the final tables
  cat(paste0("The final tables can be accessed in the database through: \n",
             data_model, " > ", schema_with_final_tables, " > ", view_or_table))

}