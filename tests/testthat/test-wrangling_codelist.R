
testthat::test_that("Basic wrangling transforms one row into two rows", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(codelist)
  
  # Should have 2 rows (one for code, one for coding_system)
  testthat::expect_equal(nrow(result), 2)
  
  # Verify columns
  expected_cols <- c("id_set", "concept_id", "cdm_table_name", "cdm_column", 
                     "code", "keep_value_column_name", "keep_date_column_name", "order_index")
  testthat::expect_setequal(names(result), expected_cols)
  
  # Verify id_set is integer
  testthat::expect_true(is.integer(result$id_set))
  
  # Verify content with default column names
  testthat::expect_equal(result$concept_id, c("WEIGHT_BIRTH", "WEIGHT_BIRTH"))
  testthat::expect_equal(result$cdm_column, c("code", "coding_system"))
  testthat::expect_equal(result$code, c("E66", "ICD10"))
  testthat::expect_equal(result$order_index, c(1, 2))
})

testthat::test_that("Auto-generated id_set is integer", {
  codelist <- data.table(
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col1", "col2"),
    keep_date_column_name = c("date1", "date2")
  )
  
  result <- wrangling_codelist(codelist)
  
  # Should have auto-generated id_set values 1, 1, 2, 2 as integers
  testthat::expect_equal(result$id_set, c(1L, 1L, 2L, 2L))
  testthat::expect_true(is.integer(result$id_set))
})

testthat::test_that("Custom integer id_set_col parameter", {
  codelist <- data.table(
    custom_id = c(10L, 20L),
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col1", "col2"),
    keep_date_column_name = c("date1", "date2")
  )
  
  result <- wrangling_codelist(codelist, id_set_col = "custom_id")
  
  # Should use custom integer id_set values
  testthat::expect_equal(result$id_set, c(10L, 10L, 20L, 20L))
  testthat::expect_true(is.integer(result$id_set))
})

testthat::test_that("Rejects non-integer id_set_col", {
  codelist <- data.table(
    custom_id = c("SET_A", "SET_B"),
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col1", "col2"),
    keep_date_column_name = c("date1", "date2")
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist, id_set_col = "custom_id"),
    "must be integer type"
  )
})

testthat::test_that("Rejects numeric (double) id_set_col", {
  codelist <- data.table(
    custom_id = c(1.5, 2.5),
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col1", "col2"),
    keep_date_column_name = c("date1", "date2")
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist, id_set_col = "custom_id"),
    "must be integer type"
  )
})

testthat::test_that("Rejects non-existent id_set_col", {
  codelist <- data.table(
    concept_id = "CONCEPT1",
    cdm_name = "CDM_A",
    cdm_table_name = "TABLE1",
    code = "CODE1",
    coding_system = "ICD10",
    keep_value_column_name = "col1",
    keep_date_column_name = "date1"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist, id_set_col = "nonexistent_col"),
    "not found in codelist"
  )
})

testthat::test_that("Custom code_column_name parameter", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "actual_code"
  )
  
  testthat::expect_equal(nrow(result), 2)
  # First row should use custom code_column_name
  testthat::expect_equal(result$cdm_column[1], "actual_code")
  # Second row should use default coding_system
  testthat::expect_equal(result$cdm_column[2], "coding_system")
})

testthat::test_that("Custom codingsystem_column_name parameter", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    codingsystem_column_name = "system_type"
  )
  
  testthat::expect_equal(nrow(result), 2)
  # First row should use default code
  testthat::expect_equal(result$cdm_column[1], "code")
  # Second row should use custom codingsystem_column_name
  testthat::expect_equal(result$cdm_column[2], "system_type")
})

testthat::test_that("Both custom column names", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "actual_code",
    codingsystem_column_name = "system_type"
  )
  
  testthat::expect_equal(nrow(result), 2)
  testthat::expect_equal(result$cdm_column, c("actual_code", "system_type"))
  testthat::expect_equal(result$code, c("E66", "ICD10"))
})

testthat::test_that("Handles NA values in keep_value_column_name", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = NA_character_,
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(codelist)
  
  testthat::expect_equal(nrow(result), 2)
  testthat::expect_true(all(is.na(result$keep_value_column_name)))
  testthat::expect_equal(result$keep_date_column_name, c("mo_date", "mo_date"))
})

testthat::test_that("Rejects NA in keep_date_column_name", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = NA_character_
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist),
    "keep_date_column_name cannot contain NA values"
  )
})

testthat::test_that("Multiple rows with mixed NA in keep_value but not keep_date", {
  codelist <- data.table(
    concept_id = c("WEIGHT_BIRTH", "HEIGHT_BIRTH"),
    cdm_name = c("CONCEPTION", "CONCEPTION"),
    cdm_table_name = c("MEDICAL_OBSERVATIONS", "MEDICAL_OBSERVATIONS"),
    code = c("E66", "E67"),
    coding_system = c("ICD10", "ICD10"),
    keep_value_column_name = c("mo_source_value", NA_character_),
    keep_date_column_name = c("mo_date", "mo_date")
  )
  
  result <- wrangling_codelist(codelist)
  
  testthat::expect_equal(nrow(result), 4)
  testthat::expect_equal(result[id_set == 1L, keep_value_column_name], 
                         c("mo_source_value", "mo_source_value"))
  testthat::expect_true(all(is.na(result[id_set == 2L, keep_value_column_name])))
})

testthat::test_that("Input validation still works with NA keep columns", {
  # Missing keep_value_column_name column entirely (not just NA)
  codelist_missing_col <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_date_column_name = "mo_date"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist_missing_col),
    "codelist is missing required columns"
  )
})

testthat::test_that("Multiple rows transformation", {
  codelist <- data.table(
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col_val_1", NA_character_),
    keep_date_column_name = c("col_date_1", "col_date_2")
  )
  
  result <- wrangling_codelist(codelist)
  
  # Should have 4 rows (2 per input row)
  testthat::expect_equal(nrow(result), 4)
  
  # Verify grouping by id_set
  testthat::expect_equal(length(unique(result$id_set)), 2)
  testthat::expect_equal(result[id_set == 1L, order_index], c(1L, 2L))
  testthat::expect_equal(result[id_set == 2L, order_index], c(1L, 2L))
})

testthat::test_that("Order index generation is correct with custom column names", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = NA_character_,
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "my_code",
    codingsystem_column_name = "my_system"
  )
  
  testthat::expect_equal(result$order_index, c(1L, 2L))
  testthat::expect_equal(result$cdm_column, c("my_code", "my_system"))
})

testthat::test_that("Output sorting is consistent", {
  codelist <- data.table(
    concept_id = c("B_CONCEPT", "A_CONCEPT"),
    cdm_name = c("CDM_B", "CDM_A"),
    cdm_table_name = c("TABLE_B", "TABLE_A"),
    code = c("B_CODE", "A_CODE"),
    coding_system = c("ATC", "ICD10"),
    keep_value_column_name = c("col_b", "col_a"),
    keep_date_column_name = c("date_b", "date_a")
  )
  
  result <- wrangling_codelist(codelist)
  
  # Verify sorting by id_set, concept_id, cdm_column
  testthat::expect_equal(
    result$cdm_column,
    c("code", "coding_system", "code", "coding_system")
  )
})

testthat::test_that("Null and missing input validation", {
  # NULL codelist
  testthat::expect_error(
    wrangling_codelist(NULL),
    "codelist is required and cannot be NULL or missing"
  )
  
  # Empty codelist
  testthat::expect_error(
    wrangling_codelist(data.table()),
    "codelist cannot be empty"
  )
})

testthat::test_that("Missing required columns validation", {
  codelist_missing_code <- data.table(
    concept_id = "TEST",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist_missing_code),
    "codelist is missing required columns"
  )
})

testthat::test_that("Data type validation for required columns", {
  codelist_wrong_type <- data.table(
    concept_id = 123,  # Should be character
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = "CODE",
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist_wrong_type),
    "must be character or factor"
  )
})

testthat::test_that("Invalid code_column_name parameter type", {
  codelist <- data.table(
    concept_id = "TEST",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = "CODE",
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist, code_column_name = 123),
    "code_column_name must be a single character value"
  )
})

testthat::test_that("Invalid codingsystem_column_name parameter type", {
  codelist <- data.table(
    concept_id = "TEST",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = "CODE",
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  testthat::expect_error(
    wrangling_codelist(codelist, codingsystem_column_name = c("system1", "system2")),
    "codingsystem_column_name must be a single character value"
  )
})

testthat::test_that("Conversion from data.frame to data.table", {
  codelist_df <- data.frame(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(codelist_df)
  
  testthat::expect_equal(nrow(result), 2)
  testthat::expect_true(is.data.table(result))
})

testthat::test_that("Special characters in code values with custom column names", {
  codelist <- data.table(
    concept_id = "SPECIAL_CHARS",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = "E66.00-1",
    coding_system = "ICD-10-CM",
    keep_value_column_name = "col-with-dash",
    keep_date_column_name = "date_col"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "code-value",
    codingsystem_column_name = "coding-system"
  )
  
  testthat::expect_equal(nrow(result), 2)
  testthat::expect_equal(result$cdm_column, c("code-value", "coding-system"))
  testthat::expect_equal(result$code, c("E66.00-1", "ICD-10-CM"))
})

testthat::test_that("Filtering of NA code values with custom names", {
  codelist <- data.table(
    concept_id = "TEST",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = NA_character_,
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "actual_code"
  )
  
  # Should only have one row (coding_system), code row should be filtered
  testthat::expect_equal(nrow(result), 1)
  testthat::expect_equal(result$cdm_column, "coding_system")
})

testthat::test_that("Filtering of empty string values with custom names", {
  codelist <- data.table(
    concept_id = "TEST",
    cdm_name = "CDM",
    cdm_table_name = "TABLE",
    code = "",
    coding_system = "ICD10",
    keep_value_column_name = "col",
    keep_date_column_name = "date"
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    codingsystem_column_name = "system_type"
  )
  
  # Should only have one row (coding_system), code row should be filtered
  testthat::expect_equal(nrow(result), 1)
  testthat::expect_equal(result$cdm_column, "system_type")
})

testthat::test_that("Complex scenario with multiple rows and all parameters", {
  codelist <- data.table(
    custom_identifier = c(100L, 200L, 100L),
    concept_id = c("CONCEPT_A", "CONCEPT_B", "CONCEPT_A"),
    cdm_name = c("CDM_V1", "CDM_V1", "CDM_V2"),
    cdm_table_name = c("TABLE_X", "TABLE_Y", "TABLE_X"),
    code = c("CODE_1", "CODE_2", NA_character_),
    coding_system = c("SYSTEM_A", "SYSTEM_B", "SYSTEM_A"),
    keep_value_column_name = c("val_col", NA_character_, "val_col"),
    keep_date_column_name = c("date_col", "date_col", "date_col")
  )
  
  result <- wrangling_codelist(
    codelist = codelist,
    code_column_name = "code_value",
    codingsystem_column_name = "system",
    id_set_col = "custom_identifier"
  )
  
  # First id_set: 2 rows (code + coding_system)
  # Second id_set: 2 rows (code + coding_system)
  # First id_set again: 1 row (only coding_system, code is NA)
  testthat::expect_equal(nrow(result), 5)
  
  # Verify id_set values are integer
  testthat::expect_true(is.integer(result$id_set))
  testthat::expect_equal(result$id_set, c(100L, 100L, 100L, 200L, 200L))
  
  # Verify custom column names
  testthat::expect_true(all(result$cdm_column %in% c("code_value", "system")))
  
  # Verify order_index resets per id_set
  testthat::expect_equal(result[id_set == 100L, order_index], c(1L, 2L, 1L))
  testthat::expect_equal(result[id_set == 200L, order_index], c(1L, 2L))
})

testthat::test_that("Factor columns in input are converted to character", {
  codelist <- data.table(
    concept_id = as.factor("WEIGHT_BIRTH"),
    cdm_name = as.factor("CONCEPTION"),
    cdm_table_name = as.factor("MEDICAL_OBSERVATIONS"),
    code = as.factor("E66"),
    coding_system = as.factor("ICD10"),
    keep_value_column_name = as.factor("mo_source_value"),
    keep_date_column_name = as.factor("mo_date")
  )
  
  result <- wrangling_codelist(codelist)
  
  # Verify all character columns are character, not factor
  testthat::expect_true(is.character(result$concept_id))
  testthat::expect_true(is.character(result$cdm_table_name))
  testthat::expect_true(is.character(result$cdm_column))
  testthat::expect_true(is.character(result$code))
  testthat::expect_true(is.character(result$keep_value_column_name))
  testthat::expect_true(is.character(result$keep_date_column_name))
})

testthat::test_that("Result is always data.table", {
  codelist <- data.table(
    concept_id = "WEIGHT_BIRTH",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    code = "E66",
    coding_system = "ICD10",
    keep_value_column_name = "mo_source_value",
    keep_date_column_name = "mo_date"
  )
  
  result <- wrangling_codelist(codelist)
  
  testthat::expect_true(is.data.table(result))
})

testthat::test_that("order_index is always integer", {
  codelist <- data.table(
    concept_id = c("CONCEPT1", "CONCEPT2"),
    cdm_name = c("CDM_A", "CDM_B"),
    cdm_table_name = c("TABLE1", "TABLE2"),
    code = c("CODE1", "CODE2"),
    coding_system = c("ICD10", "ATC"),
    keep_value_column_name = c("col1", "col2"),
    keep_date_column_name = c("date1", "date2")
  )
  
  result <- wrangling_codelist(codelist)
  
  testthat::expect_true(is.integer(result$order_index))
  testthat::expect_equal(result$order_index, c(1L, 2L, 1L, 2L))
})