test_that("VIEW clean_missing_values creates views correctly", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Sample table
  DBI::dbExecute(con, "
    CREATE TABLE PERSONS (
      person_id INTEGER,
      name TEXT
    )
  ")

  DBI::dbExecute(con, "
    INSERT INTO PERSONS VALUES
    (1, 'Alice'),
    (2, NULL),
    (3, ''),
    (4, 'NA'),
    (5, 'Bob')
  ")

  list_cols <- list(PERSONS = "name")

  # Run in view mode
  clean_missing_values(con, list_cols, to_view = TRUE)

  # Check that PERSONS_view exists
  views <- DBI::dbGetQuery(con, "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'")
  expect_true(all(c("PERSONS_T2DMM_view_1","PERSONS_T2DMM_view_2") %in% views$table_name))

  # Check data from the view
  res <- DBI::dbGetQuery(con, "SELECT * FROM PERSONS_T2DMM_view_2 ORDER BY person_id")
  expect_equal(res$person_id, c(1, 5)) # Only valid rows remain

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("clean_missing_values overwrites tables in materialized mode", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Sample table
  DBI::dbExecute(con, "
    CREATE TABLE VACCINES (
      person_id INTEGER,
      vaccine TEXT
    )
  ")

  DBI::dbExecute(con, "
    INSERT INTO VACCINES VALUES
    (1, 'A'),
    (NULL, 'B'),
    (2, ''),
    (3, 'NA'),
    (4, 'C')
  ")

  list_cols <- list(VACCINES = "vaccine")

  # Run in materialized mode
  clean_missing_values(con, list_cols)

  # Check that the table still exists under the original name
  tables <- DBI::dbListTables(con)
  expect_true("VACCINES" %in% tables)

  # Check cleaned data
  res <- DBI::dbGetQuery(con, "SELECT * FROM VACCINES ORDER BY person_id")
  expect_equal(res$person_id, c(1, 4, NA)) # Only valid rows remain

  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("clean_missing_values skips non-existing tables", {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")

  # Provide a table that does not exist
  list_cols <- list(NON_EXISTENT = "id")


  expect_message(
    clean_missing_values(con, list_cols, to_view = TRUE),
    fixed = TRUE,
    "Table NON_EXISTENT does not exist"
  )
  DBI::dbDisconnect(con, shutdown = TRUE)
})
