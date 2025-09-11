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

required_packages <- c("tictoc", "dplyr", "here", "DBI", "duckdb", "purrr",
                       "stringr")

load_and_install_packages <- function(required_packages) {
  required_packages <- required_packages[required_packages != ""]
  installed_packages <- rownames(installed.packages())

  for (pkg in required_packages) {
    if (!(pkg %in% installed_packages)) {
      install.packages(pkg)
    }
    suppressPackageStartupMessages(library(pkg, character.only = TRUE))
  }
}

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
# 7. load_data_as is either 'views' or 'tables'
# 8. verbosity is either 0 or 1
# 9. schema is correct

check_params <- function(data_model,
                         excel_path_to_cdm_schema,
                         format_source_files,
                         create_db_as,
                         verbosity,
                         folder_path_to_source_files,
                         file_path_to_target_db) {
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
    stop(paste("The folder is empty:", folder_path_to_source_files,))}

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
      con <- dbConnect(duckdb::duckdb(), dbdir = file_path_to_target_db)
      dbDisconnect(con)
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

  cat("All parameter checks passed!\n")
}

################################################################################
########################## Setup Database Connection ###########################
################################################################################

# Steps:
# 1. Remove existing target database file if it exists
# 2. Connect to the DuckDB database
# 3. Create the required schemas in the database

setup_db_connection <- function(file_path_to_target_db,
                                schema_individual_views,
                                schema_combined_views,
                                schema_conception) {
  if (file.exists(file_path_to_target_db)) {
    file.remove(file_path_to_target_db)
    warning("Removed existing import database.")
  }
  con <- dbConnect(duckdb::duckdb(), dbdir = file_path_to_target_db)

  # Create the schemas
	dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_individual_views))
	dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ", schema_combined_views))
  dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ",  schema_conception))
  message("Schemas created")

  return(con)
}

################################################################################
###################### Create SQL Views of Source Files ########################
################################################################################

# Steps:
# 1. Loop through the list of expected tables in the CDM
# 2. Check if there are files in the source folder that match the table name
# 3. If there are matching files, create a view for every file in the schema..
# 4. .. by first sanitizing the view name, then,
# 5. creating a view_filename in schema_individual_views

read_source_files_as_views <- function(folder_path_to_source_files,
                                       format_source_files,
                                       db_connection,
                                       schema_individual_views,
                                       tables_in_cdm,
                                       data_model) {
  # Some files have invalid characters in them, replace these with an "_"
  sanitize_view_name <- function(name) {
    gsub("[^a-zA-Z0-9_]+", "_", name)
  }
  tables_in_input <- c()
  # Loop through the list of expected tables in the CDM
  for (table in tables_in_cdm) {
    cat(paste0("\nCreating view for table: ", table, "..."))

    # Check if there are files in the source folder that match the table name
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
      tables_in_input <- c(tables_in_input, table)
    }

    # Create a view for each matching file
    for (file in matching_files) {
      cat(paste0("\tProcessing file: \033[3m", file, "\033[0m\n"))

      # Generate a sanitized view name
      view_name <- paste0("view_", sanitize_view_name(file_path_sans_ext(basename(file))))

      # Execute the query to create the view
      query <- paste0(
        "CREATE OR REPLACE VIEW ", data_model, ".",
        schema_individual_views, ".", view_name,
        " AS SELECT * FROM read_csv_auto('", file, "', ALL_VARCHAR = TRUE, 
        nullstr = ['NA', ''])"
      )
      tryCatch({
        dbExecute(db_connection, query)
        cat(paste0("\tView created: ", view_name, "\n"))
      }, error = function(e) {
        warning(paste0("Failed to create view for file: ", file, 
                       "\nError: ", e$message))
      })
    }

    cat(paste0("\nCreated views for table: ", table, ".\n"))
  }
  return(tables_in_input)
}

################################################################################
################ Create Empty CDM Tables with Correct Schema ###################
################################################################################

# Steps:
# 1. Define a function to generate SQL CREATE TABLE statements based on column
#    formats in a table.
# 2. Loop through each table in the CDM and read the corresponding excel sheet.
# 3. Clean the sheet data, the call the previous function for each table.
# 4. Append DDL statements for all tables.
# 5. Execute query to create empty tables in the DB according to CDM schema.

create_empty_cdm_tables <- function(excel_path_to_cdm_schema,
                                    tables_in_cdm,
                                    schema_conception,
                                    db_connection) {

  # Generate DDL (Data Definition Language) for a table
  generate_ddl <- function(df, table_name, schema_name) {
    # Map column format to DuckDB datatypes
    format_mapping <- list(
      "Numeric" = "DECIMAL(18,3)",
      "Character" = "VARCHAR",
      "Character yyyymmdd" = "DATE",
      "Integer" = "INTEGER"
    )

    # Create column definitions with fallback to VARCHAR for unknown formats
    column_definitions <- df %>%
      mutate(column_definition = paste0('"', Variable, '" ',
                                        ifelse(Format %in% names(format_mapping),
                                               format_mapping[[Format]],
                                               "VARCHAR"))) %>%
      pull(column_definition) %>%
      paste(collapse = ",\n  ")
    
    # Construct the CREATE TABLE statement
    ddl <- paste0(
      "CREATE OR REPLACE TABLE ", ".", schema_name, ".", 
      table_name, " (\n  ", column_definitions, "\n);\n\n"
    )
    ddl
  }

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
    ddl <- generate_ddl(sheet_data, table_name, schema_conception)

    # Append the DDL to the full DDL script
    if (exists("full_DDL")) {
      full_ddl <- paste0(full_ddl, ddl)
    } else {
      full_ddl <- ddl
    }

    cat("done!\n")
  }

  # Execute the full DDL which is the final SQL query
  dbExecute(db_connection, full_ddl) %>%
    cat("\rEmpty conception tables created\n")

}

################################################################################
############### Populate Empty CDM Tables with Views Generated #################
################################################################################

# Steps:

populate_cdm_tables_from_views <- function(db_connection,
                                           schema_individual_views,
                                           schema_conception,
                                           tables_in_cdm,
                                           tables_in_input
                                           file_path_to_target_db) {
  # Function to read the columns and datatypes per table
  get_table_info <- function(schema, table) {
    dbGetQuery(db_connection,
               paste0("SELECT column_name, data_type ",
                      "FROM information_schema.columns ",
                      "WHERE table_schema = '", schema,
                      "' AND table_name = '", table, "'"))
  }

allowed <- unlist(tables_in_cdm)
input_tables <- unlist(tables_in_input)
  

  # Map source views to target tables
  tables_main_df <- tables_main %>%
    mutate(target = map_chr(source,
           function(x) {
            found <- allowed[str_detect(x, fixed(allowed))]
            if (length(found) == 0) NA_character_ else found
                        })) |>
    unique()

  # Truncate the target tables to prevent putting new data into existing

for (table in trucate_tables_list) {
  query <- paste0("TRUNCATE TABLE ", schema_conception, ".", DBI::dbQuoteIdentifier(con, table))
  dbExecute(con, query)
  cat("Truncated table:", table, "\n")
}

##################################################################
#### Now we're going to load the views into the target tables ####
##################################################################

# Set the batch size, after loading batch_size number of files we'll checkpoint
# TODO: Set this dynamically depending on the size of the file we're loading. 
# i.e. batch_size <- if_else(file_size > 100 MB, 100, 10)
batch_size <- 10

# Checkpoint at 1GB
dbExecute(con, "PRAGMA wal_autocheckpoint='5GB';")

# Set the number of threads. This should be lower than the actual number of cores
# TODO: set this automatically depending on the number of cores, i.e. threads = number_of_cores - 2
dbExecute(con, "PRAGMA threads=4;")

# Start the collection of commits
DBI::dbBegin(con)

# Set counter to zero
counter <- 0

# Empty the log / create empty log file
file.create("log.txt")

for (source_view in tables_main_df$source) {
  
  # Start the timer
  tic()
  
  # Selecting the row we're working on
  working_row <- tables_main_df |>
    filter(source == source_view)
  
  # Some output if we start a new block
  if ((if (exists("target")) target else "") != working_row$target) {
    cat(paste0("\033[1m\nNow starting with the ", working_row$target, " tables.\n\033[0m"))
  }
  
  # What is the target table we're working on?
  target <- working_row$target
    
  # Some output
  cat(paste0("Materializing view ", working_row$source, " into ", working_row$target, ".\n"))
  
  # Execute the get_table_info for that table, for both schemas
  cols_main <- get_table_info(schema_individual_views, working_row$source)
  cols_conception <- get_table_info(schema_conception, working_row$target)
  
  # Intersect the columns from each schema, so we only use the columns that are both in import and target schema
  common_columns <- intersect(cols_main$column_name, cols_conception$column_name)
  
  # So what columns from the views are being ignored?
  ignored_columns <- setdiff(cols_main$column_name, cols_conception$column_name)
  
  # If there are no common_columns then skip to the next loop to prevent an SQL script error loading nothing into nothing 
  if (length(common_columns) == 0) {
    cat(paste0("\r\033[31mSkipping \033[1m", working_row$source, "\033[22m\033[31m, no common columns\033[0m, "))
    next
  }
  
  # Generate the SQL query for data transfer
  query <- paste0(
    "INSERT INTO ", database_name_import, ".", schema_conception, ".", working_row$target, " (", paste0('"', common_columns, '"', collapse = ", "), ") ",
    "SELECT ", paste(
      sapply(common_columns, function(col) {
        target_type <- cols_conception$data_type[cols_conception$column_name == col]
        if (target_type == "DATE") {
          paste0('TRY_CAST(TRY_STRPTIME("', col, '", \'%Y%m%d\') AS DATE) AS "', col, '"')
        } else {
          paste0('TRY_CAST("', col, '" AS ', target_type, ') AS "', col, '"')
        }
      }),
      collapse = ", "
    ), 
    " FROM ", database_name_import, ".", schema_individual_views, '.', working_row$source
  )
  
  # Execute the query
  dbExecute(con, query)
  
  # Some book keeping
  counter <- counter + 1
  total_files <- nrow(tables_main_df)
  percentage_files <- paste0(round(counter / total_files * 100, 2), "%")
  time_message <- invisible(capture.output(toc()$callback_msg))
  
  # Some verbosity
  cat(paste0("\rDone materializing view ", working_row$source, " into ", working_row$target, " (", percentage_files, "), ", time_message[1], ".\n"))
  
  # Also output to disk in case the UI hangs
  log_line <- paste0("Done with ", working_row$source, ', which is file number ', counter, ' / ', total_files, ' (', percentage_files, '), ', time_message[1], '.')
  write(log_line, file = "log.txt", append = TRUE)

  # Report what we ignored
  if (length(ignored_columns) > 0) {
    cat(paste0('\033[31mThe following non-matching column(s) was / were ignored: \033[1m', paste('\n\t\t', ignored_columns, collapse = " "), '\033[0m \n'))
  }
  
  ######################################################################################################
  #### If we're on the tenth file (so 10, 20, 30, ..., et cetera) we're going to optimize the table ####
  ######################################################################################################
  
#  if (counter %% 10 == 0) {
#    optimize_query <- paste0("VACUUM ", database_name_import, ".", schema_conception, ".", working_row$target, ";")
#    cat(paste0("\033[32mRunning VACUUM on table ", working_row$target, "....\033[0m\n"))
#    dbExecute(con, optimize_query)
#    }

  # I also want to stop the loop depending on the value yes in a file (because R doesn't always listen to the console stop button)
  stop <- readLines("stop.txt", warn = FALSE)
  if (stop == "yes") {
    cat("Stopping as requested by stop.txt\n")
    break
  }
  
  if (counter %% batch_size == 0) {
    DBI::dbCommit(con)
    dbExecute(con, "CHECKPOINT;")
    DBI::dbBegin(con)
    cat("\033[32m- Batch ", counter, " comitted and checkpointed -\033[0m\n")
  }
    
}

# Final commit
DBI::dbCommit(con)

}

create_parquet_from_populated_cdm_tables <- function(){}


# Do we want this in a function that's then called at the end of the script,
# or all the main code directly in the script?
main <- function(
    data_model = "conception",     # 'conception'
    excel_path_to_cdm_schema = "./ConcePTION_CDM tables v2.2.xlsx",
    format_source_files = "parquet",              # 'csv' or 'parquet'
    create_db_as = "views",                       # 'views' or 'tables'
    verbosity = 1,                                # 0 or 1
    folder_path_to_source_files = "",
    file_path_to_target_db = "") {

  if (!(create_db_as %in% c("views", "tables"))) {
    create_db_as <<- "views"
  }
  if (!(verbosity %in% c(0, 1))) {
    verbosity <<- 1
  }
  # For now, default verbosity
  verbosity <<- 1

  # What schema are we going to put the individual views to input files into?
  schema_individual_views <- "Individual_views"
  # What schema will we put the combined views of created parquet files into?
  schema_combined_views <- "Combined_views"
  # What schema will be the target CDM with the actual tables?
  schema_conception <- "Empty_Conception_tables"

  # List of expected tables in the CDM
  if (data_model == "conception") {
    tables_in_cdm <- list('VISIT_OCCURRENCE', 'EVENTS', 'MEDICINES', 'PROCEDURES', 'VACCINES', 'MEDICAL_OBSERVATIONS', 'EUROCAT'
               , 'SURVEY_ID', 'SURVEY_OBSERVATIONS', 'PERSONS', 'OBSERVATION_PERIODS', 'PERSON_RELATIONSHIPS', 'PRODUCTS'
               , 'METADATA', 'CDM_SOURCE', 'INSTANCE')
  }
  
  # Start the time
  tic()
load_and_install_packages(required_packages)

  
  # Step 1: Check if the input parameters are correct
  check_params(data_model,
               excel_path_to_cdm_schema,
               format_source_files,
               create_db_as,
               verbosity,
               folder_path_to_source_files,
               file_path_to_target_db)

  con <- setup_db_connection(file_path_to_target_db,
                                schema_individual_views,
                                schema_combined_views,
                                schema_conception)
                                
  # Step 2: Read the source files as views in DuckDB
  read_source_files_as_views(format_source_files,
                             folder_path_to_source_files,
                             schema_individual_views,
                             tables_in_cdm)

###################################################################
#### Step 3: Create the Conception SQL DDL (table definitions) ####
###################################################################

source('3. Create_conception_DDL.R')

#######################################################################################
#### Step 4: Create the actual conception tables from the DDL we created in step 3 ####
#######################################################################################

source('4. Create_the_Conception_tables.R')

#####################################################
#### Step 5: Fill the Conception tables or views ####
#####################################################

if (through_parquet != "yes") {
  source('5a. Load_into_Conception_tables.R')
} else if (through_parquet == "yes") {
  source('5b. Load_into_parquet.R')
  if (table_or_view == "view") {
    source('6a. Load_parquet_into_views.R')
  } else if (table_or_view == "table") {
    source('6b. Load_parquet_into_physical_table.R')
  }
}
 # Close the connection
  dbDisconnect(con, shutdown = TRUE); rm(con); invisible(gc())

# Stop the time
toc()
}