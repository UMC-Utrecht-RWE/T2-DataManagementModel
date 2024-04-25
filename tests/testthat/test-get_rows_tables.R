dbname <- tempfile(fileext = ".db")
db_connection_origin <- DBI::dbConnect(RSQLite::SQLite(), dbname)
load_db(
  db_connection = db_connection_origin,
  csv_path_dir = "dbtest",
  cdm_metadata = concePTION_metadata_v2,
  cdm_tables_names = c("PERSONS", "VACCINES")
)

test_that("Checking that number of rows match with original csvs", {

  count_rows_origin <- get_rows_tables(db_connection_origin)
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  expect_equal(count_rows_origin[count_rows_origin$'name' %in% 'VACCINES', 'row_count'],nrow(vx1)+nrow(vx2))
  expect_equal(unique(count_rows_origin$name),c('PERSONS','VACCINES'))
})



