test_that("wrangling_dapspecificconceptmap transforms wide to long correctly", {
  # 1. SETUP: Create a mock concept map with 2 sets of mapping columns
  mock_map <- data.table(
    concept_id = c("C1", "C2"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    keep_value_column_name = c("VAL1", "VAL2"),
    keep_date_column_name = c("DATE1", "DATE2"),
    column_name_1 = c("col_a", "col_b"),
    expected_value_1 = c("code_1", "code_2"),
    column_name_2 = c("col_c", NA),           # C2 has a missing second mapping
    expected_value_2 = c("code_3", NA)
  )
  
  # 2. EXECUTE
  result <- wrangling_dapspecificconceptmap(mock_map)
  
  # 3. VERIFY
  # Expected total rows: 3 (2 for C1, 1 for C2 because of the NA filter in your function)
  expect_equal(nrow(result), 3)
  
  # Check id_set creation
  expect_true("id_set" %in% names(result))
  expect_equal(unique(result$id_set), c(1, 2))
  
  # Check column renaming
  expect_true(all(c("cdm_column", "code", "order_index") %in% names(result)))
  
  # Check specific mapping for C1
  c1_data <- result[concept_id == "C1"]
  expect_equal(nrow(c1_data), 2)
  expect_equal(c1_data[order_index == 1, cdm_column], "col_a")
  expect_equal(c1_data[order_index == 2, cdm_column], "col_c")
  
  # Check that order_index corresponds to the column pairs
  expect_setequal(result$order_index, c(1, 2))
})

test_that("Function handles missing required columns", {
  bad_data <- data.table(concept_id = "C1") # Missing table_name, etc.
  
  expect_error(
    wrangling_dapspecificconceptmap(bad_data),
    "Missing required columns"
  )
})

test_that("Function handles missing mapping columns", {
  bad_data <- data.table(
    concept_id = "C1", 
    cdm_table_name = "T1", 
    keep_value_column_name = "V1", 
    keep_date_column_name = "D1"
  )
  # No column_name_1 or expected_value_1
  
  expect_error(
    wrangling_dapspecificconceptmap(bad_data),
    "No 'column_name_\\*' columns found"
  )
})

test_that("Function correctly filters out NA cdm_columns", {
  mock_map <- data.table(
    concept_id = "C1",
    cdm_table_name = "T1",
    keep_value_column_name = "V1",
    keep_date_column_name = "D1",
    column_name_1 = NA_character_,
    expected_value_1 = "code_1"
  )
  
  result <- wrangling_dapspecificconceptmap(mock_map)
  
  # Since column_name_1 is NA, it should be filtered out by: 
  # results_list[!is.na(cdm_column)]
  expect_equal(nrow(result), 0)
})