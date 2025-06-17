testthat::test_that("DatabaseLoader initializes with environment variables", {

  loader <- create_database_loader(config_path = "CONFIG_SET_DB")

  testthat::expect_s3_class(loader, "DatabaseLoader")
  testthat::expect_true(DBI::dbIsValid(loader$db))
  testthat::expect_true(!is.null(loader$config))
  testthat::expect_s3_class(loader$metadata, "data.table")

  DBI::dbDisconnect(loader$db)
})

testthat::test_that("DatabaseLoader runs set_database() without error", {

  loader <- create_database_loader(config_path = "CONFIG_SET_DB")

  # We'll just check that it doesn't throw
  testthat::expect_error(
    loader$set_database(),
    NA # means expect no error
  )

  # Check if the tables correspond to the ones in the folder:
  # "EVENTS", "MEDICINES", "PERSONS", "VACCINES" are created in loader$db
  testthat::expect_true(
    all(
      c("EVENTS", "MEDICINES", "PERSONS", "VACCINES")
      %in% DBI::dbListTables(loader$db)
    )
  )

  # Check if the table have the expected number of row
  testthat::expect_equal(
    DBI::dbGetQuery(loader$db, "SELECT COUNT(*) FROM EVENTS")$count,
    5
  )
  testthat::expect_equal(
    DBI::dbGetQuery(loader$db, "SELECT COUNT(*) FROM MEDICINES")$count,
    2
  )
  testthat::expect_equal(
    DBI::dbGetQuery(loader$db, "SELECT COUNT(*) FROM PERSONS")$count,
    20
  )
  testthat::expect_equal(
    DBI::dbGetQuery(loader$db, "SELECT COUNT(*) FROM VACCINES")$count,
    30
  )

  # Check if the table have the expected column names
  testthat::expect_equal(
    DBI::dbListFields(loader$db, "EVENTS"),
    c(
      "person_id", "start_date_record", "end_date_record", "event_code",
      "event_record_vocabulary", "text_linked_to_event_code", "event_free_text",
      "present_on_admission", "laterality_of_event", "meaning_of_event",
      "origin_of_event", "visit_occurrence_id")
  )
  testthat::expect_equal(
    DBI::dbListFields(loader$db, "MEDICINES"),
    c(
      "medicinal_product_id", "meaning_of_drug_record", "origin_of_drug_record",
      "date_dispensing", "date_prescription",
      "person_id", "medicinal_product_atc_code",
      "disp_number_medicinal_product",
      "presc_quantity_per_day", "presc_quantity_unit", "presc_duration_days",
      "product_lot_number", "inidication_code", "indication_code_vocabulary",
      "prescriber_speciality", "prescriber_speciality_vocabulary",
      "visit_occurrence_id"
    )
  )
  testthat::expect_equal(
    DBI::dbListFields(loader$db, "PERSONS"),
    c(
      "person_id", "day_of_birth", "month_of_birth", "year_of_birth",
      "day_of_death", "month_of_death", "year_of_death",
      "sex_at_instance_creation", "race", "country_of_birth", "quality"
    )
  )
  testthat::expect_equal(
    DBI::dbListFields(loader$db, "VACCINES"),
    c(
      "person_id", "vx_record_date", "vx_dose", "vx_manufacturer",
      "vx_atc", "vx_type", "vx_text", "origin_of_vx_record",
      "meaning_of_vx_record", "vx_lot_num", "vx_admin_date"
    )
  )
  DBI::dbDisconnect(loader$db)
})

testthat::test_that("With imported cdm_metadata", {
  testthat::expect_error(
    loader <- create_loader_from_file("CONFIG_SET_DB"),
    NA # means expect no error
  )
})

testthat::test_that("With imported cdm_metadata", {
  testthat::expect_error(
    loader <- create_loader_from_wrong_file("CONFIG_SET_DB"),
    "cdm_metadata not valid data.table object."
  )
})

testthat::test_that("With error in the loading function", {
  loader <- create_loader_from_file("CONFIG_SET_DB")
  loader$data_instance <- NULL # Simulate no database connection
  testthat::expect_error(loader$set_database(), NA)
})

testthat::test_that("Running run_db_ops", {
  loader <- create_database_loader("CONFIG_SET_DB")
  loader$set_database()
  testthat::expect_no_error(
    loader$run_db_ops()
  )
})

testthat::test_that("Absent operation", {
  loader <- create_database_loader("CONFIG_ABSENT")
  loader$set_database()
  testthat::expect_no_error(
    loader$run_db_ops()
  )
})