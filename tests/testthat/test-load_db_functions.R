testthat::test_that("check_params fails on invalid inputs", {

  testthat::expect_error(
    check_params(
      data_model = "conception",
      excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
      format_source_files = "csv",
      folder_path_to_source_files = "nonexistent_folder/",
      through_parquet = "no",
      create_db_as = "tables"
    ),
    "The folder path does not exist"
  )

  empty_dir <- tempfile()
  dir.create(empty_dir)
  testthat::expect_error(
    check_params(
      data_model = "conception",
      excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
      format_source_files = "csv",
      folder_path_to_source_files = empty_dir,
      through_parquet = "no",
      create_db_as = "tables"
    ),
    "The folder is empty"
  )
  unlink(empty_dir, recursive = TRUE)

  testthat::expect_error(
    check_params(
      data_model = "invalid_model",
      excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
      format_source_files = "csv",
      folder_path_to_source_files = "dbtest/",
      through_parquet = "no",
      create_db_as = "tables"
    ),
    "Invalid data model name"
  )

  testthat::expect_error(
    check_params(
      data_model = "conception",
      excel_path_to_cdm_schema = "nonexistent_schema.xlsx",
      format_source_files = "csv",
      folder_path_to_source_files = "dbtest/",
      through_parquet = "no",
      create_db_as = "tables"
    ),
    "Please provide a valid path to the CDM file"
  )
})

testthat::test_that("check_params passes with all valid parameters", {
  expect_output(
    check_params(
      data_model = "conception",
      excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
      format_source_files = "csv",
      folder_path_to_source_files = "dbtest/",
      through_parquet = "no",
      create_db_as = "tables"
    ),
    "All parameter checks passed!"
  )
})

testthat::test_that("create_schemas creates schemas", {
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  testthat::expect_output(
    create_schemas(
      schema_individual_views = "individual_views",
      schema_conception = "conception",
      schema_combined_views = "combined_views",
      con = con
    ),
    "Schemas created in DuckDB"
  )

  schemas <- DBI::dbGetQuery(con, "SELECT schema_name FROM information_schema.schemata")
  testthat::expect_true(all(c("individual_views", "conception", "combined_views") %in% schemas$schema_name))

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})

testthat::test_that("sanitize_view_name replaces invalid characters", {
  expect_equal(sanitize_view_name("person-table.csv"),
               "person_table_csv")
  expect_equal(sanitize_view_name("visit@occurrence.parquet"),
               "visit_occurrence_parquet")
  expect_equal(sanitize_view_name("EVENTS 2021.csv"), "EVENTS_2021_csv")
})

testthat::test_that("read_source_files_as_views creates views for tables with matching files", {
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  create_schemas(
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    con = con
  )

  testthat::expect_equal(length(
    read_source_files_as_views(
      db_connection = con,
      data_model = "conception",
      tables_in_cdm = c("NONEXISTENT"),
      format_source_files = "csv",
      folder_path_to_source_files = "dbtest/",
      schema_individual_views = "individual_views"
    )
  ), 0)

  testthat::expect_true(any(grepl("PERSONS",
    read_source_files_as_views(
      db_connection = con,
      data_model = "conception",
      tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
      format_source_files = "csv",
      folder_path_to_source_files = "dbtest/",
      schema_individual_views = "individual_views"
    )
  )))

  testthat::expect_true(any(grepl("VACCINES",
    read_source_files_as_views(
      db_connection = con,
      data_model = "conception",
      tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
      format_source_files = "parquet",
      folder_path_to_source_files = "dbtest_parquet/",
      schema_individual_views = "individual_views"
    )
  )))

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})

testthat::test_that("generate_ddl creates SQL with correct column formats", {
  df <- data.frame(
    Variable = c("id", "name", "birth_date", "count", "unknown_col"),
    Format = c("Integer", "Character", "Character yyyymmdd", "Numeric", "UnmappedFormat"),
    stringsAsFactors = FALSE
  )

  ddl <- generate_ddl(df, data_model = "conception",
                      table_name = "PERSONS", schema_name = "conception")

  testthat::expect_true(grepl("CREATE OR REPLACE TABLE", ddl))
  testthat::expect_true(grepl('"id" INTEGER', ddl))
  testthat::expect_true(grepl('"name" VARCHAR', ddl))
  testthat::expect_true(grepl('"birth_date" DATE', ddl))
  testthat::expect_true(grepl('"count" DECIMAL\\(18,3\\)', ddl))
  testthat::expect_true(grepl('"unknown_col" VARCHAR', ddl))
})

testthat::test_that("create_empty_cdm_tables creates empty tables from Excel schema", {
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  create_schemas(
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    con = con
  )
  create_empty_cdm_tables(
    db_connection = con,
    data_model = "conception",
    excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
    tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
    schema_conception = "conception"
  )

  # Check if the table was created
  tables <- DBI::dbGetQuery(con, paste0(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = '", "conception", "'"
  ))$table_name

  testthat::expect_true(all(tables %in% toupper(tables)))
  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})

testthat::test_that("get_table_info returns correct column names and types", {
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  create_schemas(
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    con = con
  )
  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TABLE conception.conception.TEST_TABLE (id INTEGER, name VARCHAR);"
  ))

  info <- get_table_info(con, schema = "conception", table = "TEST_TABLE")

  testthat::expect_equal(nrow(info), 2)
  testthat::expect_true(all(c("id", "name") %in% info$column_name))
  testthat::expect_true(all(c("INTEGER", "VARCHAR") %in% info$data_type))

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})

testthat::test_that("populate_cdm_tables_from_views works and skips columns not in CDM", {

  # run previous steps of the load_db_main function to get to this point
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  create_schemas(
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    con = con
  )
  fininput <- read_source_files_as_views(
    db_connection = con,
    data_model = "conception",
    tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
    format_source_files = "csv",
    folder_path_to_source_files = "dbtest/",
    schema_individual_views = "individual_views"
  )
  create_empty_cdm_tables(
    db_connection = con,
    data_model = "conception",
    excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
    tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
    schema_conception = "conception"
  )
  ppath <- "dbtest/intermediate_parquet"
  if (!dir.exists(ppath)) {dir.create(ppath)} else {unlink(ppath)}

  # 1. skips columns that are not in the CDM
  testthat::expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = "conception",
      schema_individual_views = "individual_views",
      schema_conception = "conception",
      files_in_input = fininput,
      through_parquet = "no",
      parquet_path = ppath
    ),
    # 'test_column' should be skipped, this is in MEDICINES_TEST_1 in dbtest
    regexp = "The following non-matching column\\(s\\).*test_column"
  )

  # 2. inserts data in other correct columns
  result <- DBI::dbGetQuery(con, paste0(
    "SELECT * FROM ", "conception", ".", "conception", ".PERSONS"
  ))
  testthat::expect_equal(nrow(result), 20)
  testthat::expect_equal(result$person_id[1], "#ID-000001089#")

  # 3. If there are no common columns at all, skips view entirely
  # Create source view with unrelated columns
  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE VIEW ", "conception", ".", "individual_views", ".view_RANDOM AS SELECT 1 AS unrelated;"
  ))
  # Create target table with different columns
  DBI::dbExecute(con, paste0(
    "CREATE OR REPLACE TABLE ", "conception", ".", "conception", ".RANDOM (id INTEGER);"
  ))
  testthat::expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = "conception",
      schema_individual_views = "individual_views",
      schema_conception = "conception",
      files_in_input = c("RANDOM" = "RANDOM"),
      through_parquet = "no",
      parquet_path = ppath
    ),
    regexp = "no common columns"
  )

  # 4. Creates parquet if through_parquet is 'yes'"
  testthat::expect_output(
    populate_cdm_tables_from_views(
      db_connection = con,
      data_model = "conception",
      schema_individual_views = "individual_views",
      schema_conception = "conception",
      files_in_input = fininput,
      through_parquet = "yes",
      parquet_path = ppath
    ),
    regexp = "Done copying view"
  )
  parquet_files <- list.files(ppath, pattern = "\\.parquet$", full.names = TRUE)
  testthat::expect_true(length(parquet_files) > 0)

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})

testthat::test_that("combine_parquet_views combines views from multiple parquet files", {
  # run previous steps of the load_db_main function to get to this point
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)
  create_schemas(
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    con = con
  )
  fininput <- read_source_files_as_views(
    db_connection = con,
    data_model = "conception",
    tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
    format_source_files = "csv",
    folder_path_to_source_files = "dbtest/",
    schema_individual_views = "individual_views"
  )
  create_empty_cdm_tables(
    db_connection = con,
    data_model = "conception",
    excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
    tables_in_cdm = c("PERSONS", "VACCINES", "MEDICINES", "EVENTS"),
    schema_conception = "conception"
  )
  ppath <- "dbtest/intermediate_parquet"
  if (!dir.exists(ppath)) {dir.create(ppath)} else {unlink(ppath)}
  populate_cdm_tables_from_views(
    db_connection = con,
    data_model = "conception",
    schema_individual_views = "individual_views",
    schema_conception = "conception",
    files_in_input = fininput,
    through_parquet = "no",
    parquet_path = ppath
  )

  combine_parquet_views(
    db_connection = con,
    data_model = "conception",
    schema_conception = "conception",
    schema_combined_views = "combined_views",
    files_in_input = fininput,
    create_db_as = "views",
    parquet_path = ppath
  )

  # Check if combined view exists
  views <- DBI::dbGetQuery(
    con, paste0("SELECT table_name FROM information_schema.views 
                     WHERE table_schema = '", "combined_views", "'")
  )$table_name

  testthat::expect_true("combined_VACCINES" %in% views)

  # Check if combined view returns expected rows
  result <- DBI::dbGetQuery(con, paste0(
    "SELECT * FROM ", "conception", ".", "combined_views", ".combined_VACCINES"
  ))
  testthat::expect_equal(nrow(result), 30)

  dbDisconnect(con, shutdown = TRUE)
  unlink(dbname)
})