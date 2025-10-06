################################################################################
############################### Loading Packages ###############################
################################################################################

# Packages used:
# DBI - For the database connection and operations
# duckdb - For connection and communication with DuckDB
# dplyr - Magic piping
# purrr - Functional programming
# here - Set our working folder
# openxlsx - To read the excel-file
# tictoc - For timing code execution
# stringr - For string manipulations

library(tidyverse) #includes purrr, dplyr, stringr
# Explicitly import dplyr functions for use in package code
importFrom(dplyr, `%>%`, mutate, pull, filter, select, cumany)
# Declare global variables to avoid R CMD check notes
utils::globalVariables(c("Variable", "Format", "column_definition"))

################################################################################
########################## Checking Input Parameters ###########################
################################################################################

# Checks done:
# 1. Does the folder with the CSV files exist?
# 2. Does the target database file already exist?
# 3. Do files in the folder exist?
# 4. Do files in the folder contain the correct extension?
#     4.1. No valid files at all
#     4.2. Some files are invalid
# 5. Is the data model name valid?
# 6. If the target database exists, can we open the connection?
# 7. create_db_as is either 'views' or 'tables'
# 8. verbosity is either 0 or 1
# 9. schema file exists
# 10. through_parquet is either 'yes' or 'no'
# 11. Invalid parameter combination of through_parquet and create_db_as

#' Validate Input Parameters for CDM Data Processing
#'
#' This function performs a series of checks to validate the input parameters
#' required for processing source data into DuckDB.
#'
#' @param data_model Character.
#'  The name of the data model to use (e.g., `"conception"`).
#' @param excel_path_to_cdm_schema Character.
#'  Full path to the Excel file containing the CDM schema definition.
#' @param format_source_files Character.
#'  Format of the source files. Must be either `"csv"` or `"parquet"`.
#' @param folder_path_to_source_files Character.
#'  Path to the folder containing the source files.
#'  Ensure the path ends with a trailing slash (`"/"`).
#' @param through_parquet Character.
#'  Indicates whether to process through parquet files.
#'  Must be either `"yes"` or `"no"`.
#' @param file_path_to_target_db Character.
#'  Full path to the target DuckDB database file to be created.
#' @param create_db_as Character.
#'  Specifies whether to create the database using `"views"` or `"tables"`.
#' @param verbosity Integer.
#'  Level of verbosity for logging.
#'  Use `0` for minimal output and `1` for detailed output.
#'
#' @return No return value.
#'  The function stops execution with an error message if any check fails.
#'  If all checks pass, a confirmation message is printed.
#'
#' @details
#' The function performs the following checks:
#' \itemize{
#'   \item Validates existence of the source folder and schema file.
#'   \item Ensures the target database file does not already exist.
#'   \item Checks for validity of source files (`.csv` or `.parquet`).
#'   \item Validates the `data_model`, `create_db_as`, `verbosity`,
#'         and `through_parquet` parameters.
#'   \item Attempts to connect to the DuckDB database.
#' }
#'
#' @examples
#' \dontrun{
#' check_params(
#'   data_model = "conception",
#'   excel_path_to_cdm_schema = "schema/cdm_schema.xlsx",
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   through_parquet = "no",
#'   file_path_to_target_db = "data/target/my_database.duckdb",
#'   create_db_as = "tables",
#'   verbosity = 1
#' )
#' }
#'
#' @keywords internal
#'
check_params <- function(
    data_model,
    excel_path_to_cdm_schema,
    format_source_files,
    folder_path_to_source_files,
    through_parquet,
    file_path_to_target_db,
    create_db_as,
    verbosity) {
  # 1. Does the folder with the CSV files exist?
  if (!dir.exists(folder_path_to_source_files)) {
    stop(paste("The folder path does not exist:", folder_path_to_source_files,
               "\nDid you make sure to end the path with a '/'?"))
  }

  # 2. Does the target database file already exist?
  if (file.exists(file_path_to_target_db)) {
    stop(paste("The target database file already exists:",
               file_path_to_target_db,
               "\nPlease delete the file or choose another path/name."))
  }

  #3. Do files in the folder exist?
  files_in_folder <- list.files(folder_path_to_source_files)
  if (length(files_in_folder) == 0) {
    stop(paste("The folder is empty:", folder_path_to_source_files))
  }

  # 4. Do files in the folder contain the correct extension?
  valid_extensions <- c("csv", "parquet")
  file_extensions <- tools::file_ext(files_in_folder)

  # 4.1. No valid files at all
  if (!any(file_extensions %in% valid_extensions)) {
    stop(paste("No files in the folder have valid extensions 
        (.csv/ .parquet)."))
  }

  # 4.2. Some files are invalid
  if (!all(file_extensions %in% valid_extensions)) {
    invalid_files <- files_in_folder[!file_extensions %in% valid_extensions]
    warning(paste("Some files in the folder do not have valid extensions 
            (.csv/ .parquet):", paste(invalid_files, collapse = ", "),
                  "\nThese files will be ignored."))
  }

  # 5. Is the data model name valid?
  valid_data_models <- c("conception")
  if (!(data_model %in% valid_data_models)) {
    stop(paste("Invalid data model name. Choose from:",
               paste(valid_data_models, collapse = ", ")))
  }

  # 6. If the target database exists, can we open the connection?
  db_message <- NULL # Declare the error output variable
  if (file.exists(file_path_to_target_db)) {
    tryCatch({
      con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file_path_to_target_db)
      DBI::dbDisconnect(con)
    }, error = function(e) {
      db_message <<- "Database file exists, but cannot open the connection, 
      it may already be in use.\n"
    })
  }
  if (!is.null(db_message)) {
    stop(db_message)
  }

  # 7. load_data_as is either 'views' or 'tables'
  if (!(create_db_as %in% c("views", "tables"))) {
    warning(paste("create_db_as must be either 'views' or 'tables'.
                  Setting to default 'views'."))
  }

  # 8. verbosity is either 0 or 1
  if (!(verbosity %in% c(0, 1))) {
    warning(paste("verbosity must be either 0 or 1.
                  Setting to default 1."))
  }

  # 9. schema file exists
  if (!file.exists(excel_path_to_cdm_schema)) {
    stop(paste("Please provide a valid path to the CDM file in excel format. 
    This is required to create the target database schema."))
  }

  # 10. through_parquet is either 'yes' or 'no'
  if (!(through_parquet %in% c("yes", "no"))) {
    warning(paste("through_parquet must be either 'yes' or 'no'.
                  Setting to default 'yes'."))
  }

  # 11. Invalid parameter combination.
  if (through_parquet == "no" && create_db_as == "views") {
    warning(paste("Invalid parameter combination.
                   through_parquet = 'no' means input files will be converted to 
                   views and then directly views will be loaded into target tables. 
                   So, the code will proceed as though create_db_as ='tables'."))
  }
  cat("All parameter checks passed!\n")
}

################################################################################
########################## Setup Database Connection ###########################
################################################################################

#' Set Up DuckDB Connection and Create Schemas
#'
#' This function initializes a DuckDB connection and creates the specified
#'  schemas for organizing views and CDM tables. If a database file already
#'  exists at the specified path, it will be removed and replaced.
#'
#' @param schema_individual_views Character.
#'  Name of the schema to store individual views.
#' @param schema_conception Character.
#'  Name of the schema to store CDM tables.
#' @param schema_combined_views Character.
#'  Name of the schema to store combined views.
#' @param file_path_to_target_db Character.
#'  Full path to the DuckDB database file to be created.
#'
#' @return A DuckDB connection object (`DBIConnection`) with the schemas.
#'
#' @examples
#' \dontrun{
#' con <- setup_db_connection(
#'   schema_individual_views = "individual_views",
#'   schema_conception = "cdm_conception",
#'   schema_combined_views = "combined_views",
#'   file_path_to_target_db = "data/target/my_database.duckdb"
#' )
#' }
#' @keywords internal
#'
setup_db_connection <- function(
    schema_individual_views,
    schema_conception,
    schema_combined_views,
    file_path_to_target_db) {
  if (file.exists(file_path_to_target_db)) {
    file.remove(file_path_to_target_db)
    warning("Removed existing import database.")
  }
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file_path_to_target_db)

  # Create the schemas
  DBI::dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ",
                             schema_individual_views))
  DBI::dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ",
                             schema_combined_views))
  DBI::dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ",
                             schema_conception))
  message("Schemas created")

  return(con)
}

################################################################################
###################### Create SQL Views of Source Files ########################
################################################################################
# Interacts with: "schema_individual_views"

# Some files have invalid characters in them, replace these with an "_"
sanitize_view_name <- function(name) {
  gsub("[^a-zA-Z0-9_]+", "_", name)
}

#' Read Source Files and Create SQL Views in DuckDB
#'
#' This function reads source files (CSV or Parquet) from a specified folder
#'  and creates SQL views in DuckDB for each file that matches the expected
#'  CDM table names.
#'
#' @param db_connection A DuckDB database connection object (`DBIConnection`).
#' @param data_model Character.
#'  The name of the data model (e.g., `"conception"`).
#' @param tables_in_cdm Character vector.
#'  List of expected table names in the CDM.
#' @param format_source_files Character.
#'  Format of the source files. Must be either `"csv"` or `"parquet"`.
#' @param folder_path_to_source_files Character.
#'  Path to the folder containing the source files.
#' @param schema_individual_views Character.
#'  Name of the schema where individual views will be created.
#'
#' @return A character vector of sanitized file names for which views were 
#'  successfully created.
#'
#' @examples
#' \dontrun{
#' tables_in_db <- read_source_files_as_views(
#'   db_connection = con,
#'   data_model = "conception",
#'   tables_in_cdm = c("person", "observation", "visit_occurrence"),
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   schema_individual_views = "individual_views"
#' )
#' }
#'
#' @keywords internal
#'
read_source_files_as_views <- function(
    db_connection,
    data_model,
    tables_in_cdm,
    format_source_files,
    folder_path_to_source_files,
    schema_individual_views) {
  # Store list of tables for which we found files
  files_in_input <- c()

  # Loop through the list of expected tables in the CDM
  for (table in tables_in_cdm) {
    cat(paste0("\nCreating view for table: ", table, "..."))

    # Check if there are files in the source folder that match the table name
    # this can be table_name_blah.csv,
    # but not blah_table_name.csv or table_name.xlsx
    matching_files <- list.files(
      path = folder_path_to_source_files,
      pattern = paste0("^", table, ".*\\.", format_source_files, "$"),
      full.names = TRUE,
      ignore.case = TRUE
    )

    # Skip this loop if there are no matching files
    if (length(matching_files) == 0) {
      cat(paste0("\r\033[31mSkipping ", table,
                 ": No matching files found.\033[0m\n"))
      next
    } else {
      # Generate a sanitized view name
      matching_files_sanitized <-
        sanitize_view_name(tools::file_path_sans_ext(basename(file)))

      # Add matching files with table name as the name
      names(matching_files_sanitized) <- rep(table, length(matching_files))
      files_in_input <- c(files_in_input, matching_files_sanitized)
    }

    # Create a view for each matching file
    for (file in matching_files_sanitized) {
      cat(paste0("\tProcessing file: \033[3m", file, "\033[0m\n"))

      # Generate a sanitized view name
      view_name <- paste0("view_", file)

      # Execute the query to create the view
      query <- paste0(
        "CREATE OR REPLACE VIEW ", data_model, ".",
        schema_individual_views, ".", view_name,
        " AS SELECT * FROM read_csv_auto('", file, "', ALL_VARCHAR = TRUE, 
        nullstr = ['NA', ''])"
      )
      tryCatch({
        DBI::dbExecute(db_connection, query)
        cat(paste0("\tView created: ", view_name, "\n"))
      }, error = function(e) {
        warning(paste0("Failed to create view for file: ", file,
                       "\nError: ", e$message))
      })
    }

    cat(paste0("\nCreated views for table: ", table, ".\n"))
  }
  return(files_in_input)
}

################################################################################
################ Create Empty CDM Tables with Correct Schema ###################
################################################################################

# Interacts with: "schema_conception"
# Steps:
# 1. Define a function to generate SQL CREATE TABLE statements based on column
#    formats in a table.
# 2. Loop through each table in the CDM and read the corresponding excel sheet.
# 3. Clean the sheet data, the call the previous function for each table.
# 4. Append DDL statements for all tables.
# 5. Execute query to create empty tables in the DB according to CDM schema.

#' Generate DDL (Data Definition Language) for a DuckDB Table
#'
#' This function creates a SQL `CREATE OR REPLACE TABLE` statement for a given
#'  table based on a data frame containing variable names and formats. It maps
#'  formats to DuckDB-compatible data types and constructs the full DDL string.
#'
#' @param df Data Frame.
#' @param data_model String.
#' @param table_name String.
#' @param schema_name String.
#'
#' @return A character string containing the full DDL SQL statement.
#'
#' @keywords internal
#'
generate_ddl <- function(
    df,
    data_model,
    table_name,
    schema_name) {
  # Map column format to DuckDB datatypes
  format_mapping <- list(
    "Numeric" = "DECIMAL(18,3)",
    "Character" = "VARCHAR",
    "Character yyyymmdd" = "DATE",
    "Integer" = "INTEGER"
  )

  # Create column definitions with fallback to VARCHAR for unknown formats
  column_definitions <- df %>%
    dplyr::mutate(column_definition = paste0('"', Variable, '" ',
                                      ifelse(Format %in% names(format_mapping),
                                              format_mapping[[Format]],
                                              "VARCHAR"))) %>%
    dplyr::pull(column_definition) %>%
    paste(collapse = ",\n  ")

  # Construct the CREATE TABLE statement
  ddl <- paste0(
    "CREATE OR REPLACE TABLE ", data_model, ".", schema_name, ".",
    table_name, " (\n  ", column_definitions, "\n);\n\n"
  )
  ddl
}

#' Create Empty CDM Tables in DuckDB
#'
#' This function reads table definitions from an Excel-based CDM schema and
#'  generates SQL DDL statements to create empty tables in a specified schema.
#'
#' @param db_connection A DuckDB database connection object (`DBIConnection`).
#' @param data_model Character.
#'  The name of the data model (e.g., `"conception"`).
#' @param excel_path_to_cdm_schema Character.
#'  Full path to the Excel file containing the CDM schema.
#' @param tables_in_cdm Character vector.
#'  List of CDM table names to be created.
#' @param schema_conception Character.
#'  Name of the schema where the CDM tables will be created.
#'
#' @return No return value.
#'  The function executes SQL statements to create empty tables
#'   in the database. A message is printed upon successful creation.
#'
#' @details
#' For each table in `tables_in_cdm`, the function:
#' \itemize{
#'   \item Reads the corresponding sheet from the Excel schema file.
#'   \item Extracts column names and formats starting from row 4.
#'   \item Stops reading at the first occurrence of the word `"Conventions"`.
#'   \item Generates SQL DDL using a helper function `generate_ddl()`.
#'   \item Executes the combined DDL to create all tables in the schema.
#' }
#'
#' @examples
#' \dontrun{
#' create_empty_cdm_tables(
#'   db_connection = con,
#'   data_model = "conception",
#'   excel_path_to_cdm_schema = "schema/cdm_schema.xlsx",
#'   tables_in_cdm = c("person", "observation", "visit_occurrence"),
#'   schema_conception = "cdm_conception"
#' )
#' }
#'
#' @keywords internal
#'
create_empty_cdm_tables <- function(
    db_connection,
    data_model,
    excel_path_to_cdm_schema,
    tables_in_cdm,
    schema_conception) {
  # Remove any existing full_DDL variable to avoid appending
  if (exists("full_ddl")) {
    rm(full_ddl)
  }

  # Loop through each table in the CDM
  for (table_name in tables_in_cdm) {
    cat(paste0("Now creating DDL for ", table_name, "...."))

    # Read the sheet for the current table
    sheet_data <- read.xlsx(excel_path_to_cdm_schema, 
                            sheet = table_name, startRow = 4) %>%
      # Remove leading/trailing whitespace
      mutate(Variable = trimws(Variable, whitespace = "[\\h\\v]")) %>%
      # Drop rows with NA in Variable
      filter(!is.na(Variable)) %>%
      # Ignore rows after first occurence of "Conventions"
      filter(!cumany(Variable == "Conventions")) %>%
      # Keep only relevant rows
      select(Variable, Format)

    # Generate the DDL for the current table
    ddl <- generate_ddl(sheet_data, data_model, table_name, schema_conception)

    # Append the DDL to the full DDL script
    if (exists("full_DDL")) {
      full_ddl <- paste0(full_ddl, ddl)
    } else {
      full_ddl <- ddl
    }

    cat("done!\n")
  }

  # Execute the full DDL which is the final SQL query
  DBI::dbExecute(db_connection, full_ddl) %>%
    cat("\rEmpty conception tables created\n")
}

################################################################################
############ Populate Empty CDM Tables (Target) with Source Views ##############
################################################################################

# Interacts with: "schema_conception", "schema_individual_views"
# Steps:
# 1. Define a function to query column names and data types for a given table.
# 2. Setup performance-related PRAGMAs (WAL checkpointing, threading, caching).
# 3. Process files in batches of fixed size using a counter:
#    for each batch, commit the database transactions, log progress to a file,
#    create a temporary folder, and perform a checkpoint.
# 4. Outer loop over target tables (files_in_input/ tables_in_cdm).
# 5. Inner loop: for each target, over all views of source files.
# 6. For each source view, retrieve column metadata for both source view
#    and target table. Identify common and ignored columns.
# 7. Transform data in source view (common columns only) to match target column
#    datatypes.
# 8. Generate SQL queries and execute.
#    IF through_parquet=='no': Insert data from source view into target table.
#    IF through_parquet=='yes': Copy data from view into a Parquet file in the
#    intermediate_parquet folder.

#' Retrieve Column Names and Data Types for a Table
#'
#' Queries the DuckDB `information_schema.columns` table to retrieve
#'  the column names and data types for a specified table within a given schema.
#'
#' @param schema Character.
#'  The name of the schema containing the table.
#' @param table Character.
#'  The name of the table for which to retrieve column metadata.
#'
#' @return A data frame with two columns: `column_name` and `data_type`.
#'
#' @examples
#' \dontrun{
#' get_table_info(schema = "cdm_conception", table = "person")
#' }
#'
#' @keywords internal
#'
get_table_info <- function(
    con,
    schema,
    table) {
  DBI::dbGetQuery(con, paste0(
    "SELECT column_name, data_type ",
    "FROM information_schema.columns ",
    "WHERE table_schema = '", schema, "' AND table_name = '", table, "'"
  ))
}

#' Populate CDM Tables from Source Views
#'
#' This function populates CDM tables in DuckDB by transforming and inserting 
#' data from previously created source views.
#'
#' @param db_connection A DuckDB database connection object (`DBIConnection`).
#' @param data_model Character.
#'  The name of the data model (e.g., `"conception"`).
#' @param schema_individual_views Character.
#'  Schema containing the source views.
#' @param schema_conception Character.
#'  Schema where the CDM tables are located.
#' @param files_in_input Named character vector.
#'  Sanitized view names as values, with CDM table names as names.
#' @param through_parquet Character.
#'  Whether to use Parquet as an intermediate step (`"yes"` or `"no"`).
#' @param file_path_to_target_db Character.
#'  Full path to the DuckDB database file.
#' @param create_db_as Character.
#'  Indicates whether the database was created using `"views"` or `"tables"`.
#'
#' @return No return value.
#'  The function inserts data into CDM tables and logs progress to `log.txt`.
#'
#' @examples
#' \dontrun{
#' populate_cdm_tables_from_views(
#'   db_connection = con,
#'   data_model = "conception",
#'   schema_individual_views = "individual_views",
#'   schema_conception = "cdm_conception",
#'   files_in_input = c(person1 = "person", person2 = "person"),
#'   through_parquet = "no",
#'   file_path_to_target_db = "data/target/my_database.duckdb",
#'   create_db_as = "tables"
#' )
#' }
#'
#' @keywords internal
#'
populate_cdm_tables_from_views <- function(
    db_connection,
    data_model,
    schema_individual_views,
    schema_conception,
    files_in_input,
    through_parquet,
    create_db_as) {

  # TODO: Ensure target tables are empty (previously by truncating them)
  # TODO: Set batch_size dynamically depending on the size of the file.
  # i.e. batch_size <- if_else(file_size > 100 MB, 100, 10)
  # TODO: set number of threads automatically depending on the number of cores, 
  # i.e. threads = number_of_cores - 2

  # How many files to process in one batch before committing and checkpointing
  batch_size <- 10

  # Write-Ahead Logging (WAL) checkpoint to occur every 5GB
  # Helps manage memory and disk usage during large inserts
  DBI::dbExecute(db_connection, "PRAGMA wal_autocheckpoint='5GB';")

  # Set the number of threads to be < number of cores
  DBI::dbExecute(db_connection, "PRAGMA threads=4;")

  # Performance tweak
  DBI::dbExecute(db_connection, "PRAGMA enable_object_cache = TRUE")

  # Start the collection of commits
  DBI::dbBegin(db_connection)

  # Set counter to zero
  counter <- 0

  # Empty the log / create empty log file
  file.create("log.txt")

  #  Ideally this should be the same as tables_in_cdm
  targets <- unique(names(files_in_input))

  for (target in targets) {
    cat(paste0("\033[1m\nNow starting with the ", target, " tables.\n\033[0m"))

    # Get all source views for this target
    source_views <- files_in_input[names(files_in_input) == target]
    for (view in source_views) {
      source_view <- paste0("view_", view)

      cat(paste0("Materializing view ", source_view, " into ", target, ".\n"))
      tictoc::tic() # Start the timer

      # Get columns in source and target tables
      cols_source <- get_table_info(schema_individual_views, source_view)
      cols_target <- get_table_info(schema_conception, target)

      # Determine common and ignored columns
      common_columns <- intersect(cols_source$column_name,
                                  cols_target$column_name)
      ignored_columns <- setdiff(cols_source$column_name,
                                 cols_target$column_name)

      # If there are no common_columns then skip to the next loop
      if (length(common_columns) == 0) {
        cat(paste0("\r\033[31mSkipping \033[1m", source_view,
                   "\033[22m\033[31m, no common columns\033[0m\n"))
        next
      }
      # Report ignored columns
      if (length(ignored_columns) > 0) {
        cat(paste0("\033[31mThe following non-matching column(s)
                    was / were ignored: \033[1m", 
                   paste("\n\t\t", ignored_columns, collapse = " "),
                   "\033[0m \n"))
      }

      if (through_parquet == "no"){
        # Build SQL query to insert into target table
        query_insert_into_target <- paste0(
          "INSERT INTO ", data_model, ".", schema_conception, ".", target, " (",
          paste0('"', common_columns, '"', collapse = ", "), ") ",
          "SELECT ", paste(
            sapply(common_columns, function(col) {
              target_type <-
                cols_target$data_type[cols_target$column_name == col]
              if (target_type == "DATE") {
                paste0('TRY_CAST(TRY_STRPTIME("', col, '", \'%Y%m%d\')
                AS DATE) AS "', col, '"')
              } else {
                paste0('TRY_CAST("', col, '" AS ', target_type, ')
                AS "', col, '"')
              }
            }),
            collapse = ", "
          ),
          " FROM ", data_model, ".", schema_individual_views, ".", source_view
        )
        DBI::dbExecute(db_connection, query_insert_into_target)
      }

      if (through_parquet == "yes") {
        # Create the temp folder if it does not exist,
        # only for counter == 0
        if (counter == 0) {
          if (!dir.exists("intermediate_parquet")) {
            dir.create("intermediate_parquet")
          } else {
            # If it exists make sure it's empty
            unlink("./intermediate_parquet/*")
          }
        }
        # Build SQL query to export to Parquet
        query_copy_into_parquet <- paste0(
          "COPY (SELECT ", paste(
            sapply(common_columns, function(col) {
              target_type <-
                cols_target$data_type[cols_target$column_name == col]
              if (target_type == "DATE") {
                paste0("TRY_CAST(TRY_STRPTIME(", col, ", '%Y%m%d')
                AS DATE) AS ", col)
              } else {
                paste0("TRY_CAST(", col, " AS ", target_type, ")
                AS ", col)
              }
            }),
            collapse = ", "
          ),
          " FROM ", data_model, ".", schema_individual_views, ".", source_view,
          ") TO 'intermediate_parquet/", view, ".parquet' (FORMAT 'parquet');"
        )
        DBI::dbExecute(db_connection, query_copy_into_parquet)
        cat(paste0("\rDone copying view ", source_view, " into parquet file."))
      }

      # Bookkeeping
      counter <- counter + 1
      total_files <- length(files_in_input)
      percentage_files <- paste0(round(counter / total_files * 100, 2), "%")
      time_message <- invisible(capture.output(tictoc::toc()$callback_msg))

      cat(paste0("\rDone transforming view ", source_view, " into ", target,
                 " (", percentage_files, "), ", time_message[1], ".\n"))
      # Log to disk
      log_line <- paste0("Done with ", source_view, ", which is file number ",
                         counter, " / ", total_files, " (", percentage_files,
                         "), ", time_message[1], ".")
      write(log_line, file = "log.txt", append = TRUE)

      # Stop condition
      stop <- readLines("stop.txt", warn = FALSE)
      if (stop == "yes") {
        cat("Stopping as requested by stop.txt\n")
        break
      }

      # Checkpoint every batch
      if (counter %% batch_size == 0) {
        DBI::dbCommit(db_connection)
        DBI::dbExecute(db_connection, "CHECKPOINT;")
        DBI::dbBegin(db_connection)
        cat("\033[32m- Batch ", counter,
            " committed and checkpointed -\033[0m\n")
      }
    }
  }
  # Final commit
  DBI::dbCommit(db_connection)
}

################################################################################
############### If through_parquet, Combine Views to create DB #################
################################################################################

# Interacts with: "schema_combined_views"

# Steps:
# 1. For each target table, extract all corresponding Parquet files.
# 2. Query the schema_conception to get correct column names and data types.
# 3. For each Parquet file, create an individual view ensuring all columns
#    from the CDM table definition are present (CAST NULL if missing).
# 4. Combine all individual views into a single combined view or table,
#    depending on create_db_as parameter.

#' Combine Parquet-Based Views into Final CDM Views or Tables
#'
#' This function creates combined views or tables in DuckDB by unifying
#'  intermediate Parquet-based views for each CDM table. It ensures column
#'  alignment with the CDM schema and handles missing columns gracefully.
#'
#' @param db_connection A DuckDB database connection object (`DBIConnection`).
#' @param data_model Character.
#'  The name of the data model (e.g., `"conception"`).
#' @param schema_conception Character.
#'  Schema where the CDM tables are defined.
#' @param schema_combined_views Character.
#'  Schema where the combined views or tables will be created.
#' @param files_in_input Named character vector.
#'  Sanitized view names as values, with CDM table names as names.
#' @param create_db_as Character.
#'  Whether to create `"views"` or `"tables"` in the combined schema.
#'
#' @return No return value.
#'  The function creates combined views or tables in the specified schema.
#'
#' @examples
#' \dontrun{
#' combine_parquet_views(
#'   db_connection = con,
#'   data_model = "conception",
#'   schema_conception = "cdm_conception",
#'   schema_combined_views = "combined_views",
#'   files_in_input = c(person1 = "person", person2 = "person"),
#'   create_db_as = "views"
#' )
#' }
#'
#' @keywords internal
combine_parquet_views <- function(
    db_connection,
    data_model,
    schema_conception,
    schema_combined_views,
    files_in_input,
    create_db_as) {
  # Get the list of targets (tables)
  targets <- unique(names(files_in_input))

  for (target in targets) {
    cat(paste0("\033[1m\nCreating combined view(s) for table ",
               target, "....\n\033[0m"))

    # Get all parquet files for this target
    parquet_files <- files_in_input[names(files_in_input) == target]
    parquet_paths <- file.path("intermediate_parquet",
                               paste0(parquet_files, ".parquet"))

    # Get reference columns/types from the CDM table definition
    ref_info <- DBI::dbGetQuery(
      db_connection,
      paste0(
        "SELECT column_name, data_type FROM information_schema.columns ",
        "WHERE table_schema = '", schema_conception,
        "' AND table_name = '", target, "'"
      )
    )
    ref_cols <- ref_info$column_name
    ref_types <- ref_info$data_type

    # For each parquet file, create an individual view
    individual_view_names <- character(length(parquet_paths))
    for (i in seq_along(parquet_paths)) {
      parquet_path <- parquet_paths[i]
      view_name <- paste0("view_", parquet_files[i])
      individual_view_names[i] <- view_name

      # Get columns in this parquet file
      parquet_info <- DBI::dbGetQuery(
        db_connection,
        sprintf(
          "DESCRIBE SELECT * FROM read_parquet('%s', union_by_name=TRUE)",
          gsub("\\\\", "/", parquet_path)
        )
      )
      parquet_cols <- parquet_info$column_name

      # The SQL expression for each column
      select_exprs <- vapply(seq_along(ref_cols), function(j) {
        col <- ref_cols[j]
        type <- ref_types[j]
        if (col %in% parquet_cols) {
          sprintf('"%s"', col)
        } else {
          sprintf('CAST(NULL AS %s) AS "%s"', type, col)
        }
      }, character(1))

      select_sql <- paste(select_exprs, collapse = ",\n  ")

      # Create the individual view for this parquet file
      query <- sprintf(
        "CREATE OR REPLACE VIEW %s.%s.%s AS
        SELECT
          %s
        FROM read_parquet('%s', union_by_name=TRUE);",
        data_model,
        schema_combined_views,
        view_name,
        select_sql,
        gsub("\\\\", "/", parquet_path)
      )
      DBI::dbExecute(db_connection, query)
      cat(paste0("\033[32mView ", view_name, " created\033[0m\n"))
    }

    # Now combine all individual views for this target
    combined_name <- paste0("combined_", target)
    union_selects <- paste(
      vapply(individual_view_names, function(vn) {
        sprintf("SELECT * FROM %s.%s.%s", data_model, schema_combined_views, vn)
      }, character(1)),
      collapse = "\nUNION ALL\n"
    )

    if (create_db_as == "views") {
      combined_query <- sprintf(
        "CREATE OR REPLACE VIEW %s.%s.%s AS\n%s;",
        data_model,
        schema_combined_views,
        combined_name,
        union_selects
      )
      DBI::dbExecute(db_connection, combined_query)
      cat(paste0("\033[32mCombined view ",
                 combined_name, " created\033[0m\n\n"))
    } else if (create_db_as == "tables") {
      # Create a physical table instead of a view
      combined_query <- sprintf(
        "CREATE OR REPLACE TABLE %s.%s.%s AS\n%s;",
        data_model,
        schema_combined_views,
        combined_name,
        union_selects
      )
      DBI::dbExecute(db_connection, combined_query)
      cat(paste0("\033[32mCombined table ",
                 combined_name, " created\033[0m\n\n"))
    }
  }
}