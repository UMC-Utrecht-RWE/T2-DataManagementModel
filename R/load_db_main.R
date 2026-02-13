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
#' @param excel_path_to_cdm_schema Character.
#'  Path to the Excel file containing the CDM schema.
#' @param format_source_files Character.
#'  Format of the source files, either `"csv"` or `"parquet"`.
#' @param folder_path_to_source_files Character.
#'  Path to the folder containing source files.
#'  Ensure the path ends with a `'/'`.
#' @param through_parquet Character.
#'  Whether to process through Parquet files (`"yes"` or `"no"`).
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
#'   excel_path_to_cdm_schema = "./ConcePTION_CDM tables v2.2.xlsx",
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   through_parquet = "no",
#'   create_db_as = "tables",
#'   tables_in_cdm = c("EVENTS", "MEDICINES", "PROCEDURES")
#' )
#' }
#'
#' @export
load_db <- function(
  con = NULL,
  data_model = "ConcePTION",
  excel_path_to_cdm_schema = "data/ConcePTION_CDM tables v2.2.xlsx",
  format_source_files = "parquet",
  folder_path_to_source_files = "",
  through_parquet = "yes",
  create_db_as = "views",
  tables_in_cdm = c()
) {
  # Sanitize input parameters
  if (!(create_db_as %in% c("views", "tables"))) {
    create_db_as <<- "views"
  }
  # # Create file paths to target db and parquet files
  # file_path_to_target_db <- paste0(folder_path_to_source_files,
  #                                  "/", data_model, ".duckdb")
  parquet_path <- file.path(folder_path_to_source_files,
                            "intermediate_parquet")
  if (through_parquet == "yes") {
    if (!dir.exists(parquet_path)) {
      dir.create(parquet_path)
    } else {
      # If it exists make sure it's empty
      unlink(parquet_path)
    }
  }

  # What schema are we going to put the individual views to input files into?
  schema_individual_views <- "Individual_views"
  # What schema will we put the combined views of created parquet files into?
  schema_combined_views <- "Combined_views"
  # What schema will be the target CDM with the actual tables?
  schema_conception <- "Empty_Conception_tables"

  # # List of expected tables in the CDM
  # if (data_model == "conception") {
  #   tables_in_cdm <- c(
  #     "CDM_SOURCE",
  #     "EVENTS",
  #     "EUROCAT",
  #     "INSTANCE",
  #     "MEDICAL_OBSERVATIONS",
  #     "MEDICINES",
  #     "METADATA",
  #     "OBSERVATION_PERIODS",
  #     "PERSON_RELATIONSHIPS",
  #     "PERSONS",
  #     "PRODUCTS",
  #     "PROCEDURES",
  #     "SURVEY_ID",
  #     "SURVEY_OBSERVATIONS",
  #     "VACCINES",
  #     "VISIT_OCCURRENCE"
  #   )
  # }

  tictoc::tic()

  # 1. Check if the input parameters are correct
  # TODO: add check for tables_in_cdm?
  cat("\033[1mStep 1: Checking if input parameters are correct...\033[0m\n")
  check_params(
    data_model,
    excel_path_to_cdm_schema,
    format_source_files,
    folder_path_to_source_files,
    through_parquet,
    create_db_as
  )

  # 2. Setup the database connection and create the required schemas
  cat("\033[1mStep 2: Creating required schemas in the database ...\033[0m\n")
  create_schemas(
    schema_individual_views,
    schema_conception,
    schema_combined_views,
    con
  )

  # 3. Read the source files as views in DuckDB
  cat("\033[1mStep 3: Reading source files as views in DuckDB...\033[0m\n")
  files_in_input <- read_source_files_as_views(
    con,
    data_model,
    tables_in_cdm,
    format_source_files,
    folder_path_to_source_files,
    schema_individual_views
  )

  # 4. Create empty CDM tables with correct schema
  cat("\033[1mStep 4: Creating empty CDM tables with correct
   schema...\033[0m\n")
  create_empty_cdm_tables(
    con,
    data_model,
    excel_path_to_cdm_schema,
    tables_in_cdm,
    schema_conception
  )

  # 5. Populate empty CDM tables with data from source views
  cat("\033[1mStep 5: Populating empty CDM tables with data from source
   views...\033[0m\n")
  populate_cdm_tables_from_views(
    con,
    data_model,
    schema_individual_views,
    schema_conception,
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
      schema_conception,
      schema_combined_views,
      files_in_input,
      create_db_as,
      parquet_path
    )
    # 7. Add missing tables as empty tables
    # Choose schema based on through_parquet
    # if 'no', then then empty table already exists in schema_conception,
    # because we created all tables in create_empty_cdm_tables()
    # if 'yes', then we need to create the empty tables in schema_combined_views
    cat("\033[1mStep 7: Adding missing tables as empty tables...\033[0m\n")
    add_missing_tables_as_empty(
      con,
      data_model,
      tables_in_cdm,
      files_in_input,
      schema_conception,
      schema_combined_views,
      create_db_as
    )
  }

  # # Close the connection
  # cat("\033[1mClosing the database connection...\033[0m\n")
  # DBI::dbDisconnect(con, shutdown = TRUE)
  # rm(con)
  # invisible(gc())
  # tictoc::toc()
  cat("\033[1mHooray! Script finished running!\033[0m\n")

  # Final message: where to find the final tables
  schema_with_final_tables <- ifelse(through_parquet == "yes",
    schema_combined_views,
    schema_conception
  )
  view_or_table <- ifelse(create_db_as == "views", "Views", "Tables")
  cat(paste0(
    "The final tables can be accessed in the database through: \n",
    data_model, " > ", schema_with_final_tables, " > ", view_or_table
  ))

  # Return output schema
  output_schema <- paste0(data_model, ".", schema_with_final_tables)
  return(output_schema)
}
