# setup
db_connection_origin <- duckdb::dbConnect(duckdb::duckdb())

test_that("database gets loaded", {
  T2.DMM:::load_db(
    db_connection = db_connection_origin,
    data_instance_path = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    cdm_tables_names = c("PERSONS", "VACCINES", "MEDICINES")
  )

  # Testing that all column names are the same
  med_db <- DBI::dbReadTable(db_connection_origin, "MEDICINES")
  med_1 <- import_file("dbtest/MEDICINES_TEST1.csv")
  med_2 <- import_file("dbtest/MEDICINES_TEST2.csv")
  expect_contains(
    names(med_db),
    concePTION_metadata_v2[TABLE %in% "MEDICINES", Variable]
  )
})

test_that("load foreign characters", {
  # latin1 character to test error message
  expect_error(
    load_db(
      db_connection = db_connection_origin,
      data_instance_path = "dbtest",
      cdm_metadata = concePTION_metadata_v2,
      cdm_tables_names = c("latin1")
    ),
    regexp = "Invalid unicode"
  )

  # now with UTF-8 encoding
  load_db(
    db_connection = db_connection_origin,
    data_instance_path = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    cdm_tables_names = c("EVENTS")
  )
  utf8_db <- DBI::dbReadTable(db_connection_origin, "EVENTS")
  expect_equal(dim(utf8_db), c(5, 12))
})

test_that("database gets loaded and mandatory columns are created", {
  # Create minimal test metadata with two mandatory columns
  test_metadata <- data.table::data.table(
    TABLE = c("PERSONS", "PERSONS"),
    Variable = c("mandatory_column_1", "mandatory_column_2"),
    Mandatory = c("Yes", "Yes"),
    Format = c("Character", "Character")
  )

  # Run the function
  T2.DMM:::load_db(
    db_connection = db_connection_origin,
    data_instance_path = "dbtest",
    cdm_metadata = test_metadata,
    cdm_tables_names = c("PERSONS")
  )

  # Get resulting columns
  cols_in_table <- DBI::dbListFields(db_connection_origin, "PERSONS")

  # Check that the mandatory columns were added
  testthat::expect_true("mandatory_column_1" %in% cols_in_table)
  testthat::expect_true("mandatory_column_2" %in% cols_in_table)
})


DBI::dbDisconnect(db_connection_origin)
