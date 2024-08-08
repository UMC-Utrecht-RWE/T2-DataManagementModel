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
  expect_contains(names(vx_db),c('Ori_ID','ROWID','Ori_Table'))
  
  persons_db <- DBI::dbReadTable(db_connection_origin,'persons')
  persons <- import_file("dbtest/PERSONS.csv")
  expect_contains(names(persons_db),c('Ori_ID','ROWID','Ori_Table'))
})

test_that("Checking the ROWID is the same as the number of rows of the table",{
  create_unique_id(db_connection_origin, cdm_tables_names = c("PERSONS"), 
                   extension_name = "", id_name = "Ori_ID", 
                   separator_id = "-", require_rowid = FALSE)
  
  persons_db <- DBI::dbReadTable(db_connection_origin,'persons')
  persons <- import_file("dbtest/PERSONS.csv")
  max_rowID <- max(persons_db$ROWID)
  expect_equal(nrow(persons),max_rowID)
})

test_that("Checking the OriTable is the same as the included table",{
  create_unique_id(db_connection_origin, cdm_tables_names = c("PERSONS"), 
                   extension_name = "", id_name = "Ori_ID", 
                   separator_id = "-", require_rowid = FALSE)
  
  persons_db <- DBI::dbReadTable(db_connection_origin,'persons')
  persons <- import_file("dbtest/PERSONS.csv")
  ori_table_name <- unique(persons_db$Ori_Table)
  expect_equal('PERSONS',ori_table_name)
})