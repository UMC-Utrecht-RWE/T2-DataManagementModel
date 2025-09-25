#' Load CDM Data Set Instance from CSV or Parquet Files into a Database
#'
#' This function is designed for loading a CDM data set instance from CSV or 
#' Parquet files into a database.
#'
#' @param db_connection A database connection object (SQLiteConnection).
#' @param data_instance_path Path to the CSV/Parquet CDM database.
#' @param cdm_metadata Data.table with Table name, Variable name, and Format.
#' @param cdm_tables_names List of CDM tables names to be imported into the DB.
#' @param extension_name String to be added to the name of the tables, useful
#'   when loading different CDM instances in the same DB.
#' @param file_format Character string specifying file format:
#' "csv", "parquet", or "auto".
#'   If "auto", the function will detect and prioritize parquet files over csv.
#'
#' @keywords internal
load_db <- function(
    db_connection,
    data_instance_path,
    cdm_metadata,
    cdm_tables_names,
    extension_name = "",
    file_format = "auto",
    schema_name = 'main') {

  # Validate file_format parameter
  if (!file_format %in% c("csv", "parquet", "auto")) {
    stop("file_format must be one of: 'csv', 'parquet', or 'auto'")
  }
  
  if(!schema_name %in% 'main'){
  # create schema if not exists
   dbExecute(db_connection, sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema_name))
  }
  
  # Loop through each table in cdm_tables_names
  for (table in cdm_tables_names) {
    # What table are we going to read
    message(paste0("[load_db]: Reading for table: ", table))

    # Function to find matching files based on format
    find_matching_files <- function(format) {
      if (format == "csv") {
        pattern <- paste0(table, ".*\\.csv$")
      } else if (format == "parquet") {
        pattern <- paste0(table, ".*\\.parquet$")
      }

      list.files(
        path = data_instance_path,
        pattern = pattern,
        full.names = TRUE
      )
    }

    # Determine which files to use based on file_format setting
    matching_files <- c()
    selected_format <- NULL

    if (file_format == "auto") {
      # Try parquet first, then csv
      parquet_files <- find_matching_files("parquet")
      csv_files <- find_matching_files("csv")

      if (length(parquet_files) > 0) {
        matching_files <- parquet_files
        selected_format <- "parquet"
      } else if (length(csv_files) > 0) {
        matching_files <- csv_files
        selected_format <- "csv"
      }
    } else {
      # Use specified format
      matching_files <- find_matching_files(file_format)
      selected_format <- file_format
    }

    # Skip this loop if there are no files to read
    if (length(matching_files) == 0) {
      message(paste0(
        "[load_db]: No files found for ", table, ", in: ", data_instance_path
      ))
      next
    }

    message(paste0(
      "[load_db]: Using ", selected_format, " format for table: ", table
      ))

    # Construct the appropriate query based on file format
    if (selected_format == "parquet") {
      # For parquet files
      query <- paste0("CREATE OR REPLACE TABLE ",schema_name,".", table, ' AS
                      SELECT * FROM read_parquet("',
                      file.path(data_instance_path, table), '*.parquet",
                      union_by_name = true );')
    } else {
      # For CSV files (original logic)
      query <- paste0("CREATE OR REPLACE TABLE ",schema_name,".", table, ' AS
                      SELECT * FROM read_csv_auto("',
                      file.path(data_instance_path, table), '*.csv",
                      union_by_name = true,
                      ALL_VARCHAR = true,
                      nullstr = "NA" );')
    }

    # Execute the query
    DBI::dbExecute(db_connection, query)

    # Yeah, done
    message(paste0(
      "[load_db]: Finished reading ", table, " (", selected_format, ")"
    ))

    # Checking if any mandatory columns are missing within the database.
    cols_in_table <- DBI::dbListFields(db_connection, DBI::Id(schema = schema_name, table = table))
    standard_cdm_table_columns <- unique(
      cdm_metadata[TABLE %in% table, Variable]
    )
    # Identify mandatory columns according to CDM specification
    mandatory_colums <- unique(
      cdm_metadata[
        TABLE %in% table & stringr::str_detect(Mandatory, "Yes") == TRUE,
        Variable
      ]
    )
    # Find mandatory columns that are missing from the loaded data
    mandatory_missing_in_db <- unique(
      mandatory_colums[!mandatory_colums %in% cols_in_table]
    )
    # Extract date and character column specifications from metadata
    date_cols <- cdm_metadata[
      TABLE %in% table & stringr::str_detect(Format, "yyyymmdd") == TRUE,
      Variable
    ]
    character_cols <- cdm_metadata[
      TABLE %in% table & stringr::str_detect(Format, "Character") == TRUE,
      Variable
    ]

    # ===== ADD MISSING MANDATORY COLUMNS =====
    # Create any mandatory columns that are missing from the data files
    if (length(mandatory_missing_in_db) > 0) {
      message(paste0(
        "[load_db]: The following mandatory columns are missing in table: ",
        table
      ))
      message(paste(mandatory_missing_in_db, collapse = ", "))

      invisible(lapply(mandatory_missing_in_db, function(new_column) {
        DBI::dbExecute(db_connection, paste0(
          "ALTER TABLE ",schema_name,".", table, " ADD COLUMN ", new_column, " VARCHAR;"
        ))
      }))
    }
    # ===== REMOVE NON-STANDARD COLUMNS =====
    # Identify columns in the data that are not part of the CDM specification
    additional_columns <- cols_in_table[
      !cols_in_table %in% standard_cdm_table_columns
    ]
    if (length(additional_columns) > 0) {
      message(paste0(
        paste0(
          "[load_db]: The following columns are not part",
          " of the CDM table but are in the files : "
        )
        , paste(additional_columns, collapse = ",")
      ))

      invisible(lapply(additional_columns, function(new_column) {
        message(paste0(
          "    Dropping column : ", new_column
        ))
        tryCatch(
                 {
                   DBI::dbExecute(db_connection, paste0(
                     "ALTER TABLE ",schema_name,".", table, " DROP COLUMN ", new_column, " ;"
                   ))
                 },
                 error = function(e) {
                   message(paste0(
                     "    ACTION FAILED "
                   ))
                 })
      }))
    }

    # ===== DATE COLUMN TYPE CONVERSION =====
    # Find date columns that actually exist in the loaded table
    available_date_cols <- cols_in_table[cols_in_table %in% date_cols]

    if (length(available_date_cols) > 0) {
      message(paste0(
        "[load_db]: The following columns are identified with date format: ",
        paste(available_date_cols, collapse = ",")
      ))
      invisible(lapply(available_date_cols, function(new_column) {
        message(paste0("Converting: ", new_column))
        tryCatch(
          {
            # For parquet files, dates might already be in proper format
            # Try direct conversion first
            DBI::dbExecute(db_connection, paste0(
              "ALTER TABLE ",schema_name,".", table, " ALTER ", new_column, " TYPE DATE;"
            ))
          },
          error = function(e) {
            # If direct conversion fails
            message(
              "      Direct conversion failed. Attempting to fix with STRPTIME."
            )

            # Handle different date formats that might come from parquet vs csv
            if (selected_format == "parquet") {
              # Parquet might have dates in different formats
              # Try common parquet date formats first
              tryCatch({
                DBI::dbExecute(db_connection, paste0(
                  "UPDATE ",schema_name,".", table,
                  " SET ", new_column,
                  " = CAST(", new_column, " AS DATE) WHERE ",
                  new_column, " IS NOT NULL;"
                ))
              }, error = function(e2) {
                # Fall back to string-based conversion
                DBI::dbExecute(db_connection, paste0(
                  "UPDATE ",schema_name,".", table,
                  " SET ", new_column, " = NULL WHERE ",
                  new_column,
                  " NOT SIMILAR TO '^[0-9]{8}$|^[0-9]{4}-[0-9]{2}-[0-9]{2}$';"
                ))

                # Handle both YYYYMMDD and YYYY-MM-DD formats
                DBI::dbExecute(db_connection, paste0(
                  "UPDATE ",schema_name,".", table,
                  " SET ", new_column, " = CASE
                  WHEN ", new_column,
                  " SIMILAR TO '^[0-9]{8}$' THEN STRPTIME(",
                  new_column, ", '%Y%m%d')
                  WHEN ", new_column,
                  " SIMILAR TO '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STRPTIME(",
                  new_column, ", '%Y-%m-%d')
                  ELSE NULL
                END WHERE ", new_column, " IS NOT NULL;"
                ))
              })
            } else {
              # CSV format handling (original logic)
              # Nullify invalid values first
              DBI::dbExecute(db_connection, paste0(
                "UPDATE ",schema_name,".", table,
                " SET ", new_column, " = NULL WHERE ",
                new_column, " NOT SIMILAR TO '^[0-9]{8}$';"
              ))

              # Apply STRPTIME to reformat valid date strings
              DBI::dbExecute(db_connection, paste0(
                "UPDATE ",schema_name,".", table,
                " SET ", new_column, " = STRPTIME(", new_column,
                ", '%Y%m%d') WHERE ", new_column, " IS NOT NULL AND ",
                new_column, " <> '';"
              ))
            }

            # Retry altering the column type to DATE
            DBI::dbExecute(db_connection, paste0(
              "ALTER TABLE ",schema_name,".", table, " ALTER ", new_column, " TYPE DATE;"
            ))

            message(
              "      Column successfully converted to DATE after fixing format."
            )
          }
        )
      }))
    }
    # ===== CHARACTER COLUMN CLEANUP =====
    # Identify character columns that are not date columns

    character_cols_not_date <- character_cols[!character_cols %in% date_cols]
    available_character_cols <- cols_in_table[
      cols_in_table %in% character_cols_not_date
    ]

    # Clean character columns by converting empty strings to NULL
    invisible(lapply(available_character_cols, function(new_column) {
      message(paste0(
        "  Cleaning character column from empty spaces: ", new_column
      ))
      tryCatch(
        {
          # Replace empty strings with NULL for proper data handling
          update_query <- paste0("UPDATE ",schema_name,".", table, "
                            SET ", new_column, " = NULL
                            WHERE ", new_column, " = '';")
          DBI::dbExecute(db_connection, update_query)
        }, error = function(e) {
          message(" ACTION FAILED ")
        }
      )
    }))
  }
}