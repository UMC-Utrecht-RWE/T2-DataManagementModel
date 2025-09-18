test_that("Checking the result is a data.table", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion

  # Define column mapping for extracting unique codes from the database
  column_info_list <- list(
    list(column_name = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist from the MEDICINES table
  unique_codelist <- T2.DMM:::get_unique_codelist(
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

  # Create DAP-specific codelist by merging unique and study codelists
  dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
    unique_codelist = unique_codelist,
    study_codelist = study_codelist
  )

  # Test assertion: Verify the result is a data.table object
  testthat::expect_true(inherits(dap_specific_codeslist, "data.table"))

})

test_that("Check expected format of the codelist and unique codelist work", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(column_name = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    T2.DMM:::get_unique_codelist(
      db_connection = db_con,
      column_info_list = column_info_list,
      tb_name = "MEDICINES"
    )[[1]]
  )

  # Test 1: Verify error when study_codelist is NULL
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = NULL
    ),
    "study_codelist is required and cannot be NULL or missing"
  )

  # Load study codelist for subsequent tests
  study_codelist <- data.table::as.data.table(
    import_file("dbtest/codelist_example_medicines.csv")
  )

  # Test 2: Verify error when unique_codelist is missing
  # required "coding_system" column
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "unique_codelist is missing required columns: coding_system"
  )

  # Test 3: Verify error when unique_codelist is NULL
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = NULL,
      study_codelist = study_codelist
    ),
    "unique_codelist is required and cannot be NULL or missing"
  )

  # Test 4: Verify error when unique_codelist is empty data.table
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = data.table(),
      study_codelist = study_codelist
    ),
    "unique_codelist cannot be empty"
  )

  # Test 5: Verify error when study_codelist is empty data.table
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = data.table()
    ),
    "study_codelist cannot be empty"
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
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "unique_codelist$coding_system must be character or factor",
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
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "study_codelist is missing required columns: coding_system",
    fixed = TRUE
  )

  # Verify error when study_codelist$coding_system has wrong data type
  # Add coding_system column with wrong type
  # (numeric instead of character/factor)
  study_codelist[, coding_system := 1]

  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "study_codelist$coding_system must be character or factor",
    fixed = TRUE
  )

})

test_that("Check expected format of the codelist and unique codelist work v2", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(column_name = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    T2.DMM:::get_unique_codelist(
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
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "study_codelist$code must be character or factor",
    fixed = TRUE
  )
}
)

test_that("Check expected format of the codelist and unique codelist work v3", {
  # Setup: Create a test database connection with MEDICINES table
  db_con <- suppressMessages(create_loaded_test_db(tables = c("MEDICINES")))
  # Ensure database connection is closed after test completion
  withr::defer(DBI::dbDisconnect(db_con))

  # Define column mapping for extracting unique codes
  column_info_list <- list(
    list(column_name = "medicinal_product_id", alias_name = "code")
  )

  # Extract unique codelist and ensure it is a data.table
  unique_codelist <- as.data.table(
    T2.DMM:::get_unique_codelist(
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
  data.table::setnames(
    x = study_codelist,
    old = "drug_abbreviation", "concept_id"
  )
  data.table::setnames(
    x = study_codelist,
    old = "product_identifier", "coding_system"
  )

  unique_codelist[, code := NULL]
  unique_codelist[, code := as.numeric(1)]

  # Test: Verify error when unique_codelist$code has wrong data type
  testthat::expect_error(
    dap_specific_codeslist <- T2.DMM::create_dap_specific_codelist(
      unique_codelist = unique_codelist,
      study_codelist = study_codelist
    ),
    "unique_codelist$code must be character or factor",
    fixed = TRUE
  )
})