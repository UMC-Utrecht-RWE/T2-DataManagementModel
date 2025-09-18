test_that("Checking if the function delete the duplicates cases using *", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  expect_equal(nrow(vx_db), nrow(vx1) + nrow(vx2))

  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = db_con, scheme, save_deleted = FALSE
  )

  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  # The first 10 rows of vx1 are duplicates of vx2
  expect_equal(nrow(vx_db), nrow(vx1))
})

test_that("Checking if the function delete the duplicates cases", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(
    rep("person_id", length(cdm_tables_names)),
    cdm_tables_names
  )
  delete_duplicates_origin(
    db_connection = db_con, scheme, save_deleted = FALSE
  )

  vx1 <- import_file("dbtest/VACCINES.csv")
  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  # The first 10 rows of vx1 are duplicates of vx2
  expect_equal(nrow(vx_db), nrow(vx1))
})

test_that("Checking if all columns exist in the scheme", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))

  cdm_tables_names <- c("PERSONS")
  scheme <- setNames(rep("test1", length(cdm_tables_names)), cdm_tables_names)
  expect_message(delete_duplicates_origin(
    db_connection = db_con, scheme, save_deleted = FALSE
  ),
  fixed = TRUE,
  paste0(
    "[delete_duplicates_origin]: Table ", "PERSONS",
    " columns -> ", "test1",
    " do not exist in the DB instance table. Removing setting."
  )
  )

})

test_that("Checking if the function reports 0 deleted cases", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))
  cdm_tables_names <- c("PERSONS")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)

  #Delete duplicates and save result
  expect_message(delete_duplicates_origin(
    db_connection = db_con,
    scheme
  ), fixed = TRUE,
  "[delete_duplicates_origin] Number of record deleted: 0"
  )


})

test_that("Checking if the function saves the results", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))
  #Check the saved result is the same as the one in the database
  vx_pre <- DBI::dbReadTable(db_con, "VACCINES")
  cdm_tables_names <- c("VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)

  save_path_csv <- tempdir()

  #Delete duplicates and save result
  suppressWarnings(
    delete_duplicates_origin(
      db_connection = db_con, scheme,
      save_deleted = TRUE,
      save_path = save_path_csv
    )
  )

  #Check the saved result is the same as the one in the database
  vx_post <- DBI::dbReadTable(db_con, "VACCINES")
  file_path <- list.files(save_path_csv)
  vx_deleted <- read.csv(paste0(
    save_path_csv, "/VACCINES_",
    format(Sys.Date(), "%Y%m%d"), ".csv"
  ))
  expect_equal(nrow(vx_pre) - nrow(vx_post), nrow(vx_deleted))
})

test_that("Checking if the function saves the results with a postfix", {
  # Load the database
  db_con <- suppressMessages(create_loaded_test_db())
  withr::defer(DBI::dbDisconnect(db_con))
  cdm_tables_names <- c("VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)

  save_path_csv <- tempdir()

  post_fix <- "test_post_fix"
  #Delete duplicates and save result
  suppressWarnings(
    delete_duplicates_origin( ## warn here
      db_connection = db_con,
      scheme = scheme,
      save_deleted = TRUE,
      save_path = save_path_csv,
      add_postfix = post_fix
    )
  )

  #Check the saved result is the same as the one in the database
  vx_post <- DBI::dbReadTable(db_con, "VACCINES")
  file_path <- list.files(save_path_csv)

  expect_true(file.exists(paste0(
    save_path_csv, "/VACCINES_",
    format(Sys.Date(), "%Y%m%d"), "_",
    post_fix, ".csv"
  )))

})

test_that("VIEW: Checking if the function delete the duplicates cases using *", {
  # Load the database
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  vx_db <- DBI::dbReadTable(db_con, "VACCINES")
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx2 <- import_file("dbtest/VACCINES2.csv")
  
  expect_equal(nrow(vx_db), nrow(vx1) + nrow(vx2))
  
  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(rep("*", length(cdm_tables_names)), cdm_tables_names)
  delete_duplicates_origin(
    db_connection = db_con, 
    scheme, 
    save_deleted = FALSE, 
    to_view = TRUE
  )
  
  vx_db <- DBI::dbReadTable(db_con, "VACCINES_T2DMM_view_2")
  # The first 10 rows of vx1 are duplicates of vx2
  expect_equal(nrow(vx_db), nrow(vx1))

})

test_that("VIEW: Checking if the function delete the duplicates cases", {
  # Load the database
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  cdm_tables_names <- c("PERSONS", "VACCINES")
  scheme <- setNames(
    rep("person_id", length(cdm_tables_names)),
    cdm_tables_names
  )
  delete_duplicates_origin(
    db_connection = db_con, scheme, 
    save_deleted = FALSE, 
    to_view = TRUE
  )
  
  vx1 <- import_file("dbtest/VACCINES.csv")
  vx_db <- DBI::dbReadTable(db_con, "VACCINES_T2DMM_view_2")
  # The first 10 rows of vx1 are duplicates of vx2
  expect_equal(nrow(vx_db), nrow(vx1))
})

test_that("VIEW: Checking if all columns exist in the scheme", {
  # Load the database
  db_con <- create_loaded_test_db()
  withr::defer(DBI::dbDisconnect(db_con))
  
  cdm_tables_names <- c("PERSONS")
  scheme <- setNames(rep("test1", length(cdm_tables_names)), cdm_tables_names)
  expect_message(delete_duplicates_origin(
    db_connection = db_con, scheme, save_deleted = FALSE, to_view = TRUE
  ),
  fixed = TRUE,
  paste0(
    "[delete_duplicates_origin]: Table ", "PERSONS",
    " columns -> ", "test1",
    " do not exist in the DB instance table. Removing setting."
  )
  )
  
})