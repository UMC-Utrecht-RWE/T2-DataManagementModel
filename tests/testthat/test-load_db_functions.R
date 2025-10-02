################################################################################
################################### SETUP ######################################
################################################################################

# Define reusable parameters
dm <- "conception"
format_c <- "csv"
format_p <- "parquet"
siv <- "individual_views"
sc <- "conception"
scv <- "combined_views"
ver <- 1

# Define testing data paths
dbpath <- tempfile(fileext = ".duckdb")
dbdir <- "dbtest/"
dbdir_parquet <- "dbtest_parquet/"
eptcs <- "dbtest/ConcePTION_CDM tables v2.2.xlsx"
tables <- c("PERSONS", "VACCINES", "MEDICINES", "EVENTS")

################################################################################
################################ check_params ##################################
################################################################################

test_that("Parameter check fails if source folder does not exist", {
  expect_error(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = "nonexistent_folder/",
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "The folder path does not exist"
  )
})

test_that("Parameter check fails if target DB file already exists", {
  file.create(dbpath)

  expect_error(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = dbdir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "The target database file already exists"
  )

  unlink(dbpath)
})

test_that("Parameter check fails if folder is empty", {
  empty_dir <- tempfile()
  dir.create(empty_dir)

  expect_error(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = empty_dir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "The folder is empty"
  )

  unlink(empty_dir, recursive = TRUE)
})

test_that("Parameter check warns on invalid file extensions", {

  expect_warning(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = dbdir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "do not have valid extensions"
  )

  unlink(test_dir, recursive = TRUE)
})

test_that("Parameter check fails on invalid data model", {
  expect_error(
    check_params(
      data_model = "invalid_model",
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = dbdir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "Invalid data model name"
  )
})

test_that("Parameter check fails if schema file does not exist", {
  expect_error(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = "nonexistent_schema.xlsx",
      format_source_files = format_c,
      folder_path_to_source_files = dbdir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "Please provide a valid path to the CDM file"
  )
})

test_that("Parameter check passes with all valid parameters", {

  expect_output(
    check_params(
      data_model = dm,
      excel_path_to_cdm_schema = eptcs,
      format_source_files = format_c,
      folder_path_to_source_files = test_dir,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables",
      verbosity = ver
    ),
    "All parameter checks passed!"
  )

  unlink(c(test_dir, schema_file), recursive = TRUE)
})

################################################################################
############################ setup_db_connection ###############################
################################################################################

test_that("Creates a new DuckDB database and schemas", {
  con <- setup_db_connection(
    schema_individual_views = siv,
    schema_conception = sc,
    schema_combined_views = scv,
    file_path_to_target_db = dbpath
  )

  # Check if connection is valid
  expect_s4_class(con, "DuckDBConnection")

  # Check if schemas exist
  schemas <- DBI::dbGetQuery(con, "SELECT schema_name FROM information_schema.schemata")
  expect_true(all(c(siv, sc, scv) %in% schemas$schema_name))

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbpath)
})

test_that("Removes existing DB file and recreates it", {
  # Create a dummy DB file
  file.create(dbpath)
  expect_true(file.exists(dbpath))

  expect_warning(
    con <- setup_db_connection(
      schema_individual_views = siv,
      schema_conception = sc,
      schema_combined_views = scv,
      file_path_to_target_db = dbpath
    ),
    "Removed existing import database"
  )

  expect_s4_class(con, "DuckDBConnection")
  dbDisconnect(con, shutdown = TRUE)
  unlink(dbpath)
})

# For further tests, we will use a valid connection
con <- setup_db_connection(
  schema_individual_views = siv,
  schema_conception = sc,
  schema_combined_views = scv,
  file_path_to_target_db = dbpath
)

################################################################################
######################## read_source_files_as_views ############################
################################################################################

test_that("sanitize_view_name replaces invalid characters", {
  expect_equal(sanitize_view_name("person-table.csv"),
               "person_table_csv")
  expect_equal(sanitize_view_name("visit@occurrence.parquet"),
               "visit_occurrence_parquet")
  expect_equal(sanitize_view_name("EVENTS 2021.csv"), "EVENTS_2021_csv")
})

test_that("Skips tables with no matching files", {
  result <- read_source_files_as_views(
    db_connection = con,
    data_model = dm,
    tables_in_cdm = c("NONEXISTENT"),
    format_source_files = format_c,
    folder_path_to_source_files = dbdir,
    schema_individual_views = siv
  )
  expect_equal(length(result), 0)
})

test_that("Creates views for matching CSV files", {
  fininput <- read_source_files_as_views(
    db_connection = con,
    data_model = dm,
    tables_in_cdm = tables,
    format_source_files = format_c,
    folder_path_to_source_files = dbdir,
    schema_individual_views = siv
  )

  expect_true(any(grepl("PERSONS", fininput)))
  unlink(temp_dir, recursive = TRUE)
})

test_that("Creates views for matching Parquet files", {
  # Since we don't want duplicate views in the same schema,
  # i.e, views created from CSV and Parquet files,
  # we will create a new DB connection for this test
  con_test <- setup_db_connection(
    schema_individual_views = siv,
    schema_conception = sc,
    schema_combined_views = scv,
    file_path_to_target_db = tempfile(fileext = ".duckdb")
  )
  result <- read_source_files_as_views(
    db_connection = con,
    data_model = dm,
    tables_in_cdm = tables,
    format_source_files = format_p,
    folder_path_to_source_files = dbdir_parquet,
    schema_individual_views = siv
  )

  expect_true(any(grepl("VACCINES", result)))
  dbDisconnect(con_test, shutdown = TRUE)
})

test_that("Handles errors in view creation gracefully", {
  # Create invalid CSV file
  temp_dir <- tempfile()
  dir.create(temp_dir)
  writeLines("not,a,valid,csv", file.path(temp_dir, "EVENTS.csv"))

  expect_warning(
    read_source_files_as_views(
      db_connection = con,
      data_model = dm,
      tables_in_cdm = c("EVENTS"),
      format_source_files = format_c,
      folder_path_to_source_files = temp_dir,
      schema_individual_views = siv
    ),
    regexp = "Failed to create view"
  )

  unlink(temp_dir, recursive = TRUE)
})

################################################################################
########################### create_empty_cdm_tables ############################
################################################################################

test_that("DDL creates correct SQL for known formats", {
  df <- data.frame(
    Variable = c("id", "name", "birth_date", "count"),
    Format = c("Integer", "Character", "Character yyyymmdd", "Numeric"),
    stringsAsFactors = FALSE
  )

  ddl <- generate_ddl(df, data_model = dm, table_name = "PERSONS", schema_name = sc)

  expect_true(grepl("CREATE OR REPLACE TABLE", ddl))
  expect_true(grepl('"id" INTEGER', ddl))
  expect_true(grepl('"name" VARCHAR', ddl))
  expect_true(grepl('"birth_date" DATE', ddl))
  expect_true(grepl('"count" DECIMAL\\(18,3\\)', ddl))
})

test_that("DDL falls back to VARCHAR for unknown formats", {
  df <- data.frame(
    Variable = c("unknown_col"),
    Format = c("UnmappedFormat"),
    stringsAsFactors = FALSE
  )

  ddl <- generate_ddl(df, data_model = dm, table_name = "UNKNOWN", schema_name = sc)

  expect_true(grepl('"unknown_col" VARCHAR', ddl))
})

test_that("Creates empty tables from Excel schema", {
  create_empty_cdm_tables(
    db_connection = con_test,
    data_model = dm,
    excel_path_to_cdm_schema = eptcs,
    tables_in_cdm = tables,
    schema_conception = sc
  )

  # Check if the table was created
  tables <- DBI::dbGetQuery(con_test, paste0(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = '", sc, "'"
  ))$table_name

  expect_true(all(tables %in% toupper(tables)))
})

################################################################################
######################## populate_cdm_tables_from_views ########################
################################################################################

test_that("get_table_info returns correct column names and types", {
  DBI::dbExecute(con, paste0(
    "CREATE TABLE ", dm, ".", sc, ".TEST_TABLE (id INTEGER, name VARCHAR);"
  ))

  info <- get_table_info(con_test, schema = sc, table = "TEST_TABLE")

  expect_equal(nrow(info), 2)
  expect_true(all(c("id", "name") %in% info$column_name))
  expect_true(all(c("INTEGER", "VARCHAR") %in% info$data_type))
})

test_that("Populates cdm tables from views and skips columns not in CDM", {
  # views already exist from previous tests in siv
  expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = dm,
      schema_individual_views = siv,
      schema_conception = sc,
      files_in_input = files_in_input,
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables"
    ),
    # 'test_column' should be skipped, this is in MEDICINES_TEST_1 in dbtest
    regexp = "The following non-matching column\\(s\\).*test_column"
  )
  # Check if data was inserted
  result <- DBI::dbGetQuery(con_test, paste0(
    "SELECT * FROM ", dm, ".", sc, ".PERSONS"
  ))
  expect_equal(nrow(result), 20)
  expect_equal(result$person_id[1], "#ID-000001089#")
})

test_that("populate_cdm_tables_from_views skips views with no common columns", {
  # Create source view with unrelated columns
  DBI::dbExecute(con, paste0(
    "CREATE VIEW ", dm, ".", siv, ".view_RANDOM AS SELECT 1 AS unrelated;"
  ))

  # Create target table with different columns
  DBI::dbExecute(con, paste0(
    "CREATE TABLE ", dm, ".", sc, ".RANDOM (id INTEGER);"
  ))

  expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = dm,
      schema_individual_views = siv,
      schema_conception = sc,
      files_in_input = c("RANDOM" = "RANDOM"),
      through_parquet = "no",
      file_path_to_target_db = dbpath,
      create_db_as = "tables"
    ),
    regexp = "no common columns"
  )
})

test_that("Creates parquet if through_parquet is 'yes'", {
  expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = dm,
      schema_individual_views = siv,
      schema_conception = sc,
      files_in_input = files_in_input,
      through_parquet = "yes",
      file_path_to_target_db = dbpath,
      create_db_as = "views"
    ),
    regexp = "Done copying view into parquet file"
  )

  # Check if Parquet files were created
  parquet_files <- list.files("intermediate_parquet", pattern = "\\.parquet$", full.names = TRUE)
  expect_true(length(parquet_files) > 0)
})

################################################################################
############################ combine_parquet_views #############################
################################################################################

test_that("Creates combined view from multiple parquet files", {
  combine_parquet_views(
    db_connection = con,
    data_model = dm,
    schema_conception = sc,
    schema_combined_views = scv,
    files_in_input = fininput,
    create_db_as = "views"
  )

  # Check if combined view exists
  views <- DBI::dbGetQuery(
    con_test, paste0("SELECT table_name FROM information_schema.views 
                     WHERE table_schema = '", scv, "'")
  )$table_name

  expect_true("combined_VACCINES" %in% views)

  # Check if combined view returns expected rows
  result <- DBI::dbGetQuery(con_test, paste0(
    "SELECT * FROM ", dm, ".", scv, ".combined_VACCINES"
  ))
  expect_equal(nrow(result), 30)
})