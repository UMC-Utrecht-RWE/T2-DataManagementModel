# setup
dbname <- tempfile(fileext = ".db")
db_connection_origin <- DBI::dbConnect(RSQLite::SQLite(), dbname)

test_that("database gets loaded", {
  expect_equal(file.size(dbname), 0)

  load_db(
    db_connection = db_connection_origin,
    csv_path_dir = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    cdm_tables_names = c("PERSONS", "VACCINES", "MEDICINES")
  )

  # Testing that all column names are the same
  med_db <- DBI::dbReadTable(db_connection_origin, "MEDICINES")
  med_1 <- import_file("dbtest/MEDICINES_TEST1.csv")
  med_2 <- import_file("dbtest/MEDICINES_TEST2.csv")
  expect_contains(names(med_db), concePTION_metadata_v2[TABLE %in% "MEDICINES", Variable])
})

DBI::dbDisconnect(db_connection_origin)
