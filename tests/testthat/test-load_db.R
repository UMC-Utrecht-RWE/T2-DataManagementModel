# setup
dbname <- tempfile(fileext = ".db")
db_connection_origin <- DBI::dbConnect(RSQLite::SQLite(), dbname)

test_that("database gets loaded", {
  
  expect_equal(file.size(dbname), 0)
  
  load_db(
    db_connection = db_connection_origin,
    csv_path_dir = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    cdm_tables_names = c("PERSONS", "VACCINES")
  )
  expect_equal(file.size(dbname), 12288)
  # 12288 is the size of the database file once created.
  # TODO: create more robust tests for this database.
})

