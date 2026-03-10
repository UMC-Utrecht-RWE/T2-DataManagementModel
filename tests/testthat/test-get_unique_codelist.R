testthat::test_that("get_unique_codelist: success cases and structure", {
  # 1. Setup Mock Database
  db <- DBI::dbConnect(duckdb::duckdb(), dbname = ":memory:")
  test_data <- data.frame(
    event_code = c("A", "A", "B"),
    vocab = c("ICD10", "ICD10", "CPT"),
    free_text = c("Text1", "Text2", "Text1")
  )
  DBI::dbWriteTable(db, "events", test_data)

  # 2. Define complex input (multi-column and single-column)
  column_info_list <- list(
    # Case: Grouping by two columns into two aliases
    list(
      source_column = c("event_code", "vocab"),
      alias_name = c("code", "system")
    ),
    # Case: Simple single column
    list(
      source_column = "free_text",
      alias_name = "description"
    )
  )

  # 3. Run Function
  result_list <- get_unique_codelist(db, column_info_list, "events")

  # --- Assertions ---
  testthat::expect_length(result_list, 2)
  testthat::expect_s3_class(result_list[[1]], "data.table")

  # Check Column Names (Aliases)
  testthat::expect_named(result_list[[1]], c("code", "system", "COUNT"))
  testthat::expect_named(result_list[[2]], c("description", "COUNT"))

  # Check Logic: "A" + "ICD10" appears twice in the raw data
  # We expect a count of 2 for that specific row
  res1 <- result_list[[1]]
  count_val <- res1[code == "A" & system == "ICD10", COUNT]
  testthat::expect_equal(count_val, 2)

  DBI::dbDisconnect(db, shutdown = TRUE)
})

testthat::test_that("get_unique_codelist: validation checks", {
  # Setup empty connection for validation only
  db <- DBI::dbConnect(duckdb::duckdb(), dbname = ":memory:")

  # Error 1: Wrong key names (using the old 'column_name')
  bad_keys <- list(list(column_name = "id", alias_name = "id"))
  testthat::expect_error(
    get_unique_codelist(db, bad_keys, "table"),
    regexp = "is missing columns: 'source_column' and/or 'alias_name'."
  )

  # Error 2: Mismatched lengths (2 sources, 1 alias)
  mismatched <- list(list(
    source_column = c("col1", "col2"),
    alias_name = "alias1"
  ))
  testthat::expect_error(
    get_unique_codelist(db, mismatched, "table"),
    "must have the same length"
  )

  # Error 3: Empty list
  testthat::expect_error(
    get_unique_codelist(db, list(), "table"),
    "must be a non-empty list"
  )

  DBI::dbDisconnect(db, shutdown = TRUE)
})
