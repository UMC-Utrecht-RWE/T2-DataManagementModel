test_that("Names and number of new columns added", {
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  create_unique_id(db_con,
    cdm_tables_names = c("PERSONS", "VACCINES"),
    extension_name = ""
  )

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  expect_contains(names(vx_db), c( "unique_id", "ori_table"))

  persons_db <- DBI::dbReadTable(db_con, "persons")
  expect_contains(names(persons_db), c( "unique_id", "ori_table"))
})

test_that("Checking the OriTable is the same as the included table", {
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  create_unique_id(db_con,
    cdm_tables_names = c("PERSONS"),
    extension_name = ""
  )


  persons_db <- DBI::dbReadTable(db_con, "persons")
  persons <- import_file("dbtest/PERSONS.csv")
  ori_table_name <- unique(persons_db$ori_table)
  expect_equal("PERSONS", ori_table_name)
})
