################################################################################
########################## Checking Input Parameters ###########################
################################################################################

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
#' @param create_db_as Character.
#'  Specifies whether to create the database using `"views"` or `"tables"`.
#'
#' @return No return value.
#'  The function stops execution with an error message if any check fails.
#'  If all checks pass, a confirmation message is printed.
#'
#' @examples
#' \dontrun{
#' check_params(
#'   data_model = "conception",
#'   excel_path_to_cdm_schema = "schema/cdm_schema.xlsx",
#'   format_source_files = "csv",
#'   folder_path_to_source_files = "data/source/",
#'   through_parquet = "no",
#'   create_db_as = "tables",
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
  create_db_as
) {
  # 1. Does the folder with the CSV files exist?
  if (!dir.exists(folder_path_to_source_files)) {
    stop(paste(
      "The folder path does not exist:", folder_path_to_source_files
    ))
  }

  # 2. Do files in the folder exist?
  files_in_folder <- list.files(folder_path_to_source_files)
  if (length(files_in_folder) == 0) {
    stop(paste("The folder is empty:", folder_path_to_source_files))
  }

  # 3. Do files in the folder contain the correct extension?
  valid_extensions <- c("csv", "parquet")
  file_extensions <- tools::file_ext(files_in_folder)

  # 3.1. No valid files at all
  if (!any(file_extensions %in% valid_extensions)) {
    stop(paste("No files in the folder have valid extensions
        (.csv/ .parquet)."))
  }

  # 3.2. Some files are invalid
  if (!all(file_extensions %in% valid_extensions)) {
    invalid_files <- files_in_folder[!file_extensions %in% valid_extensions]
    cat(paste(
      "WARNING: Some files in the folder do not have valid extensions
            (.csv/ .parquet):", paste(invalid_files, collapse = ", "),
      ". These files will be ignored. \n"
    ))
  }

  # 4. Is the data model name valid?
  if (!grepl("^conception$", data_model, ignore.case = TRUE)) {
    stop(paste(
      "Invalid data model name. 
      Currently, the only supported data model is 'conception'."))
  }

  # 5. create_db_as is either 'views' or 'tables'
  if (!(create_db_as %in% c("views", "tables"))) {
    cat(paste("WARNING: create_db_as must be either 'views' or 'tables'.
                  Setting to default 'views'. \n"))
  }

  # 6. schema file exists
  if (!file.exists(excel_path_to_cdm_schema)) {
    stop(paste("Please provide a valid path to the CDM file in excel format.
    This is required to create the target database schema."))
  }

  # 7. through_parquet is either 'yes' or 'no'
  if (!(through_parquet %in% c("yes", "no"))) {
    cat(paste("WARNING: through_parquet must be either 'yes' or 'no'.
                  Setting to default 'yes'. \n"))
  }

  # 8. Invalid parameter combination.
  if (through_parquet == "no" && create_db_as == "views") {
    cat(paste("WARNING: 
              Invalid parameter combination. through_parquet = 'no' means
              that input files will be converted to views after which views
              will directly be loaded into target tables.
              So, the code will proceed as though create_db_as ='tables'. \n"))
  }
  cat("All parameter checks passed!\n")
}

################################################################################
########################## Setup Database Connection ###########################
################################################################################

#' Create Schemas
#'
#' This function initializes a DuckDB connection and creates the specified
#'  schemas for organizing views and CDM tables.
#'
#' @param schema_individual_views Character.
#'  Name of the schema to store individual views.
#' @param schema_conception Character.
#'  Name of the schema to store CDM tables.
#' @param schema_combined_views Character.
#'  Name of the schema to store combined views.
#' @param con DBIConnection.
#'  The function will create schemas in this database.
#'
#' @examples
#' \dontrun{
#' con <- setup_db_connection(
#'   schema_individual_views = "individual_views",
#'   schema_conception = "cdm_conception",
#'   schema_combined_views = "combined_views",
#'   con = db_con
#' )
#' }
#' @keywords internal
#'
create_schemas <- function(
    schema_individual_views,
    schema_conception,
    schema_combined_views,
    con) {
  # Create the schemas
  DBI::dbExecute(con, paste0(
    "CREATE SCHEMA IF NOT EXISTS ",
    schema_individual_views
  ))
  DBI::dbExecute(con, paste0(
    "CREATE SCHEMA IF NOT EXISTS ",
    schema_combined_views
  ))
  DBI::dbExecute(con, paste0(
    "CREATE SCHEMA IF NOT EXISTS ",
    schema_conception
  ))
  cat("Schemas created in DuckDB. \n")
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
      cat(paste0("Skipping ", table, ": No matching files found.\n"))
      next
    } else {
      cat(paste0("Creating view for table: ", table, "\n"))
    }

    # Create a view for each matching file
    for (file in matching_files) {
      cat(paste0("\tProcessing file: \033[3m", file, "\033[0m\n"))

      # Generate a sanitized view name
      sanitized_name <-
        sanitize_view_name(tools::file_path_sans_ext(basename(file)))
      view_name <- paste0("view_", sanitized_name)
      names(sanitized_name) <- table
      files_in_input <- c(files_in_input, sanitized_name)

      # Execute the query to create the view
      if (format_source_files == "parquet") {
        query <- paste0(
          "CREATE OR REPLACE VIEW ", data_model, ".",
          schema_individual_views, ".", view_name,
          " AS SELECT * FROM read_parquet('", file, "')"
        )
      } else if (format_source_files == "csv") {
        query <- paste0(
          "CREATE OR REPLACE VIEW ", data_model, ".",
          schema_individual_views, ".", view_name,
          " AS SELECT * FROM read_csv_auto('", file, "', ALL_VARCHAR = TRUE,
          nullstr = ['NA', ''])"
      )}
      tryCatch(
        {
          DBI::dbExecute(db_connection, query)
          cat(paste0("\tView created: ", view_name, "\n"))
        },
        warning = function(e) {
          warning(paste0(
            "Failed to create view for file: ", file,
            "\nError: ", e$message
          ))
        }
      )
    }

    cat(paste0("\nCreated views for table: ", table, ".\n"))
  }
  files_in_input
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
    dplyr::mutate(column_definition = paste0(
      '"', Variable, '" ',
      ifelse(Format %in% names(format_mapping),
        format_mapping[Format],
        "VARCHAR"
      )
    )) %>%
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
  full_ddl <- ""
  # Loop through each table in the CDM
  for (table_name in tables_in_cdm) {
    cat(paste0("Now creating DDL for ", table_name, "\n"))

    # Read the sheet for the current table
    sheet_data <- openxlsx::read.xlsx(excel_path_to_cdm_schema,
      sheet = table_name, startRow = 4
    ) %>%
      # Remove leading/trailing whitespace
      dplyr::mutate(Variable = trimws(Variable, whitespace = "[\\h\\v]")) %>%
      # Drop rows with NA in Variable
      dplyr::filter(!is.na(Variable)) %>%
      # Ignore rows after first occurence of "Conventions"
      dplyr::filter(!dplyr::cumany(Variable == "Conventions")) %>%
      # Keep only relevant rows
      dplyr::select(Variable, Format)

    # Generate the DDL for the current table
    ddl <- generate_ddl(sheet_data, data_model, table_name, schema_conception)
    # Append the DDL to the full DDL script
    full_ddl <- paste0(full_ddl, ddl)
  }

  # Execute the full DDL which is the final SQL query
  DBI::dbExecute(db_connection, full_ddl) %>%
    cat("\nEmpty conception tables created\n")
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
#' @param parquet_path Character.
#'  Path to the folder to put intermediate parquet files in.
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
#'   parquet_path = "dataset/intermediate_parquet"
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
    parquet_path
  ) {
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
      cols_source <- get_table_info(db_connection, schema_individual_views,
                                    source_view)
      cols_target <- get_table_info(db_connection, schema_conception, target)

      # Determine common and ignored columns
      common_columns <- intersect(
        cols_source$column_name,
        cols_target$column_name
      )
      ignored_columns <- setdiff(
        cols_source$column_name,
        cols_target$column_name
      )

      # If there are no common_columns then skip to the next loop
      if (length(common_columns) == 0) {
        cat(paste0(
          "\r\033[31mSkipping \033[1m", source_view,
          "\033[22m\033[31m, no common columns\033[0m\n"
        ))
        next
      }
      # Report ignored columns
      if (length(ignored_columns) > 0) {
        cat(paste0(
          "\033[31mThe following non-matching column(s)
                    was / were ignored: \033[1m",
          paste("\n\t\t", ignored_columns, collapse = " "),
          "\033[0m \n"
        ))
      }

      if (through_parquet == "no") {
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
          ") TO '", parquet_path, "/", view, ".parquet' (FORMAT 'parquet');"
        )
        DBI::dbExecute(db_connection, query_copy_into_parquet)
        cat(paste0("\rDone copying view ", source_view, " into parquet file."))
      }

      # Bookkeeping
      counter <- counter + 1
      total_files <- length(files_in_input)
      percentage_files <- paste0(round(counter / total_files * 100, 2), "%")
      time_message <- invisible(capture.output(tictoc::toc()$callback_msg))

      cat(paste0(
        "\rDone transforming view ", source_view, " into ", target,
        ". Source view is file number ",
        counter, " / ", total_files, " (", percentage_files,
        "), ", time_message[1], "."
      ))

      # Checkpoint every batch
      if (counter %% batch_size == 0) {
        DBI::dbCommit(db_connection)
        DBI::dbExecute(db_connection, "CHECKPOINT;")
        DBI::dbBegin(db_connection)
        cat(
          "\033[32m- Batch ", counter,
          " committed and checkpointed -\033[0m\n"
        )
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
#' @param parquet_path Character.
#'  Path to the folder containing intermediate parquet files.
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
#'   create_db_as = "views",
#'   parquet_path = "dataset/intermediate_parquet"
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
    create_db_as,
    parquet_path) {
  # Get the list of targets (tables)
  targets <- unique(names(files_in_input))

  for (target in targets) {
    cat(paste0(
      "\033[1m\nCreating combined view(s) for table ",
      target, "....\n\033[0m"
    ))

    # Get all parquet files for this target
    parquet_files <- files_in_input[names(files_in_input) == target]

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
    individual_view_names <- character(length(parquet_files))
    for (i in seq_along(parquet_files)) {
      file_path <- file.path(parquet_path,
                             paste0(parquet_files[i], ".parquet"))
      view_name <- paste0("view_", parquet_files[i])
      individual_view_names[i] <- view_name

      # Get columns in this parquet file
      parquet_info <- DBI::dbGetQuery(
        db_connection,
        sprintf(
          "DESCRIBE SELECT * FROM read_parquet('%s', union_by_name=TRUE)",
          file_path
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
        gsub("\\\\", "/", file_path)
      )
      DBI::dbExecute(db_connection, query)
      cat(paste0("\033[32mView ", view_name, " created\033[0m\n"))
    }

    # Now combine all individual views for this target
    # combined_name <- paste0("combined_", target)
    combined_name <- target
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
      cat(paste0(
        "\033[32mCombined view ",
        combined_name, " created\033[0m\n\n"
      ))
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
      cat(paste0(
        "\033[32mCombined table ",
        combined_name, " created\033[0m\n\n"
      ))
    }
  }
}

################################################################################
############### Add Missing Tables as Empty Tables in Target ###################
################################################################################

#' Add Missing Tables as Empty Views or Tables
#'
#' This function checks for missing tables in the input files compared to the
#'  expected tables in the CDM, and creates empty views or tables for them in
#'  the database.
#'
#' @param con A DBI connection object to the target database.
#' @param data_model String.
#'  The name of the data model (e.g., `"conception"`).
#' @param tables_in_cdm Character vector.
#'  List of expected table names in the CDM.
#' @param files_in_input Character.
#'  Named character vector of target and source files.
#' @param schema_conception Character.
#'  Schema where the CDM tables are defined.
#' @param schema_combined_views Character.
#'  Schema where the combined views or tables will be created.
#' @param create_db_as Character.
#'  Whether to create `"views"` or `"tables"` in the combined schema.
#'
#' @return No return value. The function performs side effects by executing
#'  SQL statements to create views or tables.
#'
#' @examples
#' \dontrun{
#' add_missing_tables_as_empty(
#'   con = db_connection,
#'   data_model = "my_model",
#'   tables_in_cdm = c("person", "visit_occurrence"),
#'   files_in_input = list(person = "person.csv"),
#'   schema_conception = "cdm_schema",
#'   schema_combined_views = "combined_schema",
#'   create_db_as = "views"
#' )
#' }
#'
#' @keywords internal
add_missing_tables_as_empty <- function(
    con,
    data_model,
    tables_in_cdm,
    files_in_input,
    schema_conception,
    schema_combined_views,
    create_db_as) {
  view_or_table <- ifelse(create_db_as == "views", "VIEW", "TABLE")

  # Identify missing tables
  existing_tables <- unique(names(files_in_input))
  missing_tables <- setdiff(tables_in_cdm, existing_tables)
  if (length(missing_tables) == 0) {
    cat("No missing tables to add.\n")
    return()
  } else {
    cat(paste0(
      "Adding missing tables as empty ", tolower(view_or_table), "s: ",
      paste(missing_tables, collapse = ", "), "\n"
    ))

    for (table in missing_tables) {
      query <- sprintf(
        "CREATE %s %s.%s.view_%s AS SELECT * FROM %s.%s.%s WHERE 1=0;",
        view_or_table,
        data_model, schema_combined_views, table,
        data_model, schema_conception, table
      )
      DBI::dbExecute(con, query)
    }
  }
}
