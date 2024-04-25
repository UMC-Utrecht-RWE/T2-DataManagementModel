dbname <- tempfile(fileext = ".db")
db_connection_origin <- DBI::dbConnect(RSQLite::SQLite(), dbname)
load_db(
  db_connection = db_connection_origin,
  csv_path_dir = "dbtest",
  cdm_metadata = concePTION_metadata_v2,
  cdm_tables_names = c("PERSONS", "VACCINES")
)

test_that("Names and number of new columns added", {
  
  create_unique_id(db_connection_origin, cdm_tables_names = c("PERSONS", "VACCINES"), 
                              extension_name = "", id_name = "Ori_ID", 
                              separator_id = "-", require_rowid = FALSE)
  
  vx_db <- DBI::dbReadTable(db_connection_origin,'VACCINES')
  vx <- import_file("dbtest/VACCINES.csv")
  expect_contains(names(vx_db),names(vx))
  expect_contains(names(vx_db),c('Ori_ID','ROWID','Ori_Table'))
  expect_equal(length(names(vx_db)),length(names(vx))+3)
})