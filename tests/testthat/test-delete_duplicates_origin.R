dbname <- tempfile(fileext = ".db")
db_connection_origin <- DBI::dbConnect(RSQLite::SQLite(), dbname)
load_db(
  db_connection = db_connection_origin,
  csv_path_dir = "dbtest",
  cdm_metadata = concePTION_metadata_v2,
  cdm_tables_names = c("PERSONS", "VACCINES")
)

test_that("Checking if the function delete the duplicates cases", {
  vx_db <- DBI::dbReadTable(db_connection_origin, "VACCINES")
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  expect_equal(nrow(vx_db), nrow(vx1) + nrow(vx2))

  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = db_connection_origin, scheme, save_deleted = FALSE
  )

  vx_db <- DBI::dbReadTable(db_connection_origin, "VACCINES")
  expect_equal(nrow(vx_db), nrow(vx1))
})
