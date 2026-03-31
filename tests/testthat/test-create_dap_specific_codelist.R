testthat::test_that("Checking the result is a data.table", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion

  # Define column mapping for extracting unique codes from the database
  column_info_list <- list(
    list(source_column = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist from the MEDICINES table
  unique_codelist <- get_unique_codelist(
    db_connection = db_con,
    column_info_list = column_info_list,
    tb_name = "MEDICINES"
  )[[1]]
  unique_codelist <- unique_codelist[!is.na(unique_codelist$code), ]

  # MEDICINES table is a mix of long-formatted and wide-formatted information,
  # codes are hosted in a wide-formatted field where they miss the coding system
  # Therefore, we manually add the coding system identifier
  unique_codelist[, coding_system := "PRODCODEID"]

  # Load study-specific codelist from CSV file for comparison/mapping
  study_codelist <- import_file("dbtest/codelist_example_medicines.csv")

  # Standardize column names to match expected format
  # Rename "drug_abbreviation" to "concept_id" for consistency
  data.table::setnames(
    x = study_codelist, old = "drug_abbreviation", "concept_id"
  )
  # Rename "product_identifier" to "coding_system" for consistency
  data.table::setnames(
    x = study_codelist, old = "product_identifier", "coding_system"
  )

  # set priority
  study_codelist <- unique(study_codelist)[, priority := ifelse(tags == "narrow", 1, 2)][,cdm_name := "CDM1"][, cdm_table_name := "MEDICINES"]

  # Create DAP-specific codelist by merging unique and study codelists
  dap_specific_codeslist <- create_dap_specific_codelist(
    dap_codes = unique_codelist,
    codelist = study_codelist
  )

  # Test assertion: Verify the result is a data.table object
  testthat::expect_true(inherits(dap_specific_codeslist, "data.table"))

})

testthat::test_that("Check expected format of the codelist and unique codelist work", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(source_column = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    get_unique_codelist(
      db_connection = db_con,
      column_info_list = column_info_list,
      tb_name = "MEDICINES"
    )[[1]]
  )

  # Test 1: Verify error when study_codelist is NULL
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  NULL
    ),
    "codelist is required and cannot be NULL or missing"
  )

  # Load study codelist for subsequent tests
  study_codelist <- data.table::as.data.table(
    import_file("dbtest/codelist_example_medicines.csv")
  )
  study_codelist <- study_codelist[, priority := ifelse(tags == "narrow", 1, 2)][,cdm_name := "CDM1"][, cdm_table_name := "MEDICINES"]
  # Test 2: Verify error when unique_codelist is missing
  # required "coding_system" column
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "dap_codes is missing required columns: coding_system"
  )

  # Test 3: Verify error when unique_codelist is NULL
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = NULL,
      codelist =  study_codelist
    ),
    "dap_codes is required and cannot be NULL or missing"
  )

  # Test 4: Verify error when unique_codelist is empty data.table
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = data.table(),
      codelist =  study_codelist
    ),
    "dap_codes cannot be empty"
  )

  # Test 5: Verify error when study_codelist is empty data.table
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  data.table()
    ),
    "codelist cannot be empty"
  )

  # Prepare data for type validation tests
  # Add coding_system column with wrong type
  # (numeric instead of character/factor)
  unique_codelist[, coding_system := 1]
  # Standardize study_codelist column names
  data.table::setnames(
    x = study_codelist,
    old = "drug_abbreviation", "concept_id"
  )
  data.table::setnames(
    x = study_codelist,
    old = "product_identifier", "coding_system"
  )

  # Test 6: Verify error when unique_codelist$coding_system
  # has wrong data type
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "coding_system must be character",
    fixed = TRUE
  )

  # Fix unique_codelist coding_system type for next tests
  unique_codelist[, coding_system := NULL][
    ,
    coding_system := "PRODCODEID"
  ]
  # Break study_codelist by removing coding_system column
  data.table::setnames(
    x = study_codelist,
    old = "coding_system",
    new = "product_identifier"
  )

  # Verify error when study_codelist is missing required
  # "coding_system" column
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "codelist is missing required columns: coding_system",
    fixed = TRUE
  )

  # Verify error when study_codelist$coding_system has wrong data type
  # Add coding_system column with wrong type
  # (numeric instead of character/factor)
  study_codelist[, coding_system := 1]

  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "coding_system must be character",
    fixed = TRUE
  )

})

testthat::test_that("Check expected format of the codelist and unique codelist work v2", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(source_column = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    get_unique_codelist(
      db_connection = db_con,
      column_info_list = column_info_list,
      tb_name = "MEDICINES"
    )[[1]]
  )
  unique_codelist[, coding_system := "PRODCODEID"]

  # Load study codelist for subsequent tests
  study_codelist <- data.table::as.data.table(
    import_file("dbtest/codelist_example_medicines.csv")
  )
  study_codelist <- study_codelist[, priority := ifelse(tags == "narrow", 1, 2)][,cdm_name := "CDM1"][, cdm_table_name := "MEDICINES"]
  data.table::setnames(
    x = study_codelist, old = "drug_abbreviation", "concept_id"
  )
  data.table::setnames(
    x = study_codelist, old = "product_identifier", "coding_system"
  )

  # Prepare for code column type validation tests
  # Backup original code column and replace with wrong type
  study_codelist[, code := NULL]
  study_codelist[, code := as.integer(1)]

  # Test 9: Verify error when study_codelist$code has wrong data type
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "code must be character",
    fixed = TRUE
  )
}
)

testthat::test_that("Check expected format of the codelist and unique codelist work v3", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(source_column = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    get_unique_codelist(
      db_connection = db_con,
      column_info_list = column_info_list,
      tb_name = "MEDICINES"
    )[[1]]
  )
  unique_codelist[, coding_system := "PRODCODEID"]

  # Load study codelist for subsequent tests
  study_codelist <- data.table::as.data.table(
    import_file("dbtest/codelist_example_medicines.csv")
  )
  study_codelist <- study_codelist[, priority := ifelse(tags == "narrow", 1, 2)]
  data.table::setnames(
    x = study_codelist,
    old = "drug_abbreviation", "concept_id"
  )
  data.table::setnames(
    x = study_codelist,
    old = "product_identifier", "coding_system"
  )
  study_codelist <- unique(study_codelist)[, priority := ifelse(tags == "narrow", 1, 2)][,cdm_name := "CDM1"][, cdm_table_name := "MEDICINES"]
  unique_codelist[, code := NULL]
  unique_codelist[, code := as.numeric(1)]

  # Test: Verify error when unique_codelist$code has wrong data type
  testthat::expect_error(
    dap_specific_codeslist <- create_dap_specific_codelist(
      dap_codes = unique_codelist,
      codelist =  study_codelist
    ),
    "code must be character",
    fixed = TRUE
  )
})

testthat::test_that("Start-with matching logic works for ATC codes", {
  # Setup: ATC codes are in the default start_with_colls
  unique_codelist <- data.table(
    coding_system = "ATC",
    code = c("N02BE01", "B01AC06"), # Child codes
    COUNT = c(10, 5),
    source_column = "drug_usage"
  )

  study_codelist <- data.table(
    coding_system = "ATC",
    code = c("N02B"), # Parent code
    concept_id = "Paracetamol",
    tags = "narrow"
  )
  study_codelist <- unique(study_codelist)[, priority := ifelse(tags == "narrow", 1, 2)][,cdm_name := "CDM1"][, cdm_table_name := "MEDICINES"]

  result <- create_dap_specific_codelist(
    dap_codes = unique_codelist,
    codelist =  study_codelist
  )

  # Check that N02BE01 was matched to N02B
  match_row <- result[code.dap_codes == "N02BE01"]
  expect_equal(match_row$match_status, "MATCHED")
  expect_equal(match_row$concept_id, "Paracetamol")

  # Check that B01AC06 was NOT matched (should be CDM only)
  missing_row <- result[code.dap_codes == "B01AC06"]
  expect_equal(missing_row$match_status, "ONLY_IN_DATA")
})

testthat::test_that("Priority column correctly breaks ties in matches", {
  unique_codelist <- data.table(
    coding_system = "ATC",
    code = "N02BE01",
    COUNT = 1,
    source_column = "test"
  )

  # Two study codes could match the same DAP code
  study_codelist <- data.table(cdm_name = "CDM1",
                              cdm_table_name = "MEDICINES",
                              coding_system = "ATC",
                              code = c("N02B", "N02B"),
                              concept_id = c("Paracetamol", "Paracetamol"),
                              tags = c("narrow","broad"),
                              priority_val = c(1, 2) # Assume 1 is higher priority
  )

  # Test with priority column
  result <- create_dap_specific_codelist(
    dap_codes = unique_codelist,
    codelist = study_codelist,
    priority = "priority_val"
  )

  # Should pick the one with priority 1
  expect_equal(result[code.codelist == "N02B" & tags == "narrow"]$match_status, "MATCHED")
})

testthat::test_that("match_status column correctly identifies ONLY_IN_DATA, ONLY_IN_CODELIST, and MATCHED", {
  unique_codelist <- data.table(
    coding_system = "SYSTEM1",
    code = c("MATCH", "CODE_ONLY_IN_DATA"),
    COUNT = 1,
    source_column = "test"
  )

  study_codelist <- data.table(
    cdm_name = "CDM1",
    cdm_table_name = "TABLE1",
    coding_system = "SYSTEM1",
    code = c("MATCH", "CODE_ONLY_IN_CODELIST"),
    concept_id = "test_id"
  )
  study_codelist <- study_codelist[, priority := 1]
  result <- create_dap_specific_codelist(dap_codes = unique_codelist, codelist = study_codelist)

  expect_equal(result[code.dap_codes == "MATCH" & code == "MATCH"]$match_status, "MATCHED")
  expect_equal(result[code.dap_codes == "CODE_ONLY_IN_DATA"]$match_status, "ONLY_IN_DATA")
  expect_equal(result[code.codelist == "CODE_ONLY_IN_CODELIST"]$match_status, "ONLY_IN_CODELIST")
})
