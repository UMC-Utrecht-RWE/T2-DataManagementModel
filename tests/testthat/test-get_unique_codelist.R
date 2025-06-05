testthat::test_that("get_unique_codelist generates correct SQL queries", {
  column_info_list <- list(
    list(column_name = "id", alias_name = "table_id"),
    list(column_name = "codes", alias_name = "codes")
  )

  db <- DBI::dbConnect(duckdb::duckdb(), dbname = tempfile(fileext = ".duckdb"))
  DBI::dbWriteTable(db, "my_table", data.frame(
    id = 1:5,
    codes = Sys.Date() + 1:5
  ))

  result_list <- get_unique_codelist(
    db_connection = db, column_info_list, tb_name = "my_table"
  )

  # Test that the results are data tables
  testthat::expect_true(inherits(result_list[[1]], "data.table"))
  testthat::expect_true(inherits(result_list[[2]], "data.table"))

  # Test that the results contain the expected columns
  testthat::expect_named(result_list[[1]], c("table_id", "COUNT"))
  testthat::expect_named(result_list[[2]], c("codes", "COUNT"))

  # Test that the counts are correct
  testthat::expect_equal(result_list[[1]]$COUNT, rep(1, 5))
  testthat::expect_equal(result_list[[2]]$COUNT, rep(1, 5))

  DBI::dbDisconnect(db)
})