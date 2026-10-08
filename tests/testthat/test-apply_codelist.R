test_that("apply_codelist performs input validation", {
  con <- dbConnect(duckdb::duckdb(), ":memory:")

  closed_con <- dbConnect(duckdb::duckdb(), ":memory:")
  dbDisconnect(closed_con, shutdown = TRUE)
  expect_error(
    apply_codelist(closed_con, data.table(), materialize = "in_database"),
    "database connection is not valid"
  )

  # Test invalid data table
  expect_error(
    apply_codelist(
      con,
      data.frame(a = 1),
      materialize = "in_database"
    ), "must be a data.table"
  )

  # Test empty data table
  expect_error(
    apply_codelist(con, data.table(), materialize = "in_database"),
    "codelist is empty"
  )

  # Test missing columns
  incomplete_dt <- data.table(concept_id = 1)
  expect_error(
    apply_codelist(
      con,
      incomplete_dt,
      materialize = "in_database"
    ),
    "Missing required columns"
  )

  valid_cols <- c(
    "cdm_table_name", "cdm_column", "concept_id", "code",
    "order_index", "keep_value_column_name", "keep_date_column_name"
  )

  # Helper to create a valid base object
  get_valid_dt <- function() {
    dt <- data.table(matrix(nrow = 1, ncol = length(valid_cols)))
    setnames(dt, valid_cols)
    dt[, order_index := 1L] # Must be numeric
    dt
  }

  bad_type_dt <- get_valid_dt()
  bad_type_dt[, order_index := "not_a_number"]
  expect_error(
    apply_codelist(con, bad_type_dt, materialize = "in_database"),
    "'order_index' must be numeric/integer."
  )

  # --- 4. Materialization & Path Logic ---
  expect_error(
    apply_codelist(con, get_valid_dt(), materialize = "invalid_string"),
    "must be either 'in_parquet' or 'in_database'"
  )

  expect_error(
    apply_codelist(
      con,
      get_valid_dt(),
      materialize = "in_parquet",
      path_parquets = NULL
    ),
    regexp = "'path_parquets' needed if materialize = 'in_parquet'"
  )
  expect_error(
    apply_codelist(
      con,
      get_valid_dt(),
      materialize = "in_parquet",
      path_parquets = 1L
    ),
    "'path_parquets' must be a single character string"
  )
  expect_error(
    apply_codelist(
      con,
      get_valid_dt(),
      materialize = "in_parquet",
      path_parquets = c("first", "second")
    ),
    "'path_parquets' must be a single character string"
  )

  # --- 5. Warnings and missing source table validation ---
  warnings <- capture_warnings(
    expect_error(
      apply_codelist(
        con,
        get_valid_dt(),
        materialize = "in_database",
        path_parquets = "ignored/path"
      ),
      "cdm_table_name.*missing names"
    )
  )

  expect_length(warnings, 1)

  expect_true(any(grepl(" 'path_parquets' ignored if ", warnings)))


  dbDisconnect(con, shutdown = TRUE)
})

test_that("apply_codelist executes hierarchical SQL flow", {
  db_path <- tempfile(fileext = "testDB.duckdb")
  # 1. SETUP: Create temporary DB and mock CDM table
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  load_db(
    con = con,
    data_model = "ConcePTION",
    cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.yaml",
    format_source_files = "csv",
    folder_path_to_source_files = local_dbtest_copy(),
    create_db_as = "tables",
    tables_in_cdm = c("PERSONS", "EVENTS", "MEDICINES", "MEDICAL_OBSERVATIONS")
  )

  DBI::dbExecute(
    con,
    "CREATE TABLE TEST_EVENTS AS
      SELECT *
        ,UUID() AS unique_id
        ,'EVENTS' AS ori_table
      FROM ConcePTION.EVENTS"
  )

  # 2. SETUP: Create a hierarchical codelist
  # We have one family with a parent (order 1) and a child (order 2)
  test_codelist <- data.table(
    id_set = c(1, 1), # Added: Wrangling function always adds this
    concept_id = c("covid_test", "covid_test"),
    cdm_table_name = "TEST_EVENTS",
    cdm_column = c("event_code", "event_record_vocabulary"),
    code = c("U07.1", "ICD10CM"),
    keep_value_column_name = c("event_code", "event_code"),
    keep_date_column_name = c("start_date_record", "start_date_record"),
    order_index = c(1, 2) # L suffix ensures Integer type
  )

  list_tables <- dbListTables(con)
  expect_contains(list_tables, "PERSONS")
  # 3. EXECUTE: We wrap in capture_messages to check the flow
  msgs <- capture_messages(apply_codelist(
    db_con = con,
    scheme = NULL,
    codelist = test_codelist,
    materialize = "in_database"
  ))

  # 4. VERIFY: Check if the logic branched correctly
  expect_match(msgs[2], "Applying parent scheme")
  expect_match(
    msgs[3],
    "Processing child with cdm_column=event_record_vocabulary"
  )

  # Verify that the temporary table 'codelist' was created in the DB during the
  #  process (Note: In a real run, it might be dropped, but we check logic flow
  #  via messages)
  expect_true(any(grepl("Order index: 2", msgs)))

  # Verifying that the only identificable case was identified
  concept_table <- dbReadTable(con, "concept_table")
  expect_equal(nrow(concept_table), 1)
  expect_equal(concept_table$person_id, "21110000001")

  dbDisconnect(con, shutdown = TRUE)

  con <- dbConnect(duckdb(), db_path)
  list_tables <- dbListTables(con)
  expect_contains(list_tables, "PERSONS")

  temp_parquet_path <- withr::local_tempdir()

  # 3. EXECUTE: We wrap in capture_messages to check the flow
  msgs <- capture_messages(apply_codelist(
    db_con = con,
    scheme = NULL,
    codelist = test_codelist,
    materialize = "in_parquet",
    path_parquets = temp_parquet_path
  ))

  # 4. VERIFY: Check if the logic branched correctly
  expect_match(msgs[2], "Applying parent scheme")
  expect_match(
    msgs[3],
    "Processing child with cdm_column=event_record_vocabulary"
  )

  # Verify that the temporary table 'codelist' was created in the DB
  # (Note: In a real run, it might be dropped, but we check logic
  # flow via messages)
  expect_true(any(grepl("Order index: 2", msgs)))

  # Verifying that the only identificable case was identified
  concept_table <- dbReadTable(con, "concept_table")
  expect_equal(nrow(concept_table), 1)
  expect_equal(concept_table$person_id, "21110000001")

  dbDisconnect(con, shutdown = TRUE)
})

# p1: N02 / ATC / level A, p2: product P1, p3: N02 / ATC / level B
create_medicines_test_db <- function(env = parent.frame()) {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  DBI::dbExecute(con, "
    CREATE TABLE MED (
      unique_id UUID,
      ori_table VARCHAR,
      person_id VARCHAR,
      atc VARCHAR,
      prod VARCHAR,
      sys VARCHAR,
      lvl VARCHAR,
      d DATE
    )")
  DBI::dbExecute(con, "
    INSERT INTO MED VALUES
      ('00000000-0000-0000-0000-000000000001', 'MED', 'p1', 'N02', 'X', 'ATC', 'A', '2020-01-01'),
      ('00000000-0000-0000-0000-000000000002', 'MED', 'p2', 'Z99', 'P1', 'ATC', 'A', '2020-01-01'),
      ('00000000-0000-0000-0000-000000000003', 'MED', 'p3', 'N02', 'X', 'ATC', 'B', '2020-01-01')")
  con
}

test_that("apply_codelist applies conditions with order_index > 2", {
  con <- create_medicines_test_db()

  # One id_set with three possibilities: atc = N02 AND sys = ATC AND lvl = A
  codelist <- data.table(
    id_set = 1L,
    concept_id = "C1",
    cdm_table_name = "MED",
    cdm_column = c("atc", "sys", "lvl"),
    code = c("N02", "ATC", "A"),
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = 1:3
  )

  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )

  concept_table <- dbGetQuery(
    con, "SELECT person_id FROM concept_table ORDER BY person_id"
  )
  # p3 has lvl = B, so the third condition must exclude it
  expect_equal(concept_table$person_id, "p1")
})

test_that("apply_codelist applies child levels when a level is skipped", {
  con <- create_medicines_test_db()

  # Levels 1 and 3 with no level 2: atc = N02 AND lvl = A
  codelist <- data.table(
    id_set = 1L,
    concept_id = "C1",
    cdm_table_name = "MED",
    cdm_column = c("atc", "lvl"),
    code = c("N02", "A"),
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = c(1L, 3L)
  )

  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )

  concept_table <- dbGetQuery(
    con, "SELECT person_id FROM concept_table ORDER BY person_id"
  )
  # p3 has lvl = B, so the level-3 condition must exclude it
  expect_equal(concept_table$person_id, "p1")
})

test_that("apply_codelist rejects a mixed set of present and missing tables", {
  con <- create_medicines_test_db()
  codelist <- data.table(
    id_set = c(1L, 2L),
    concept_id = c("C1", "C2"),
    cdm_table_name = c("MED", "MISSING"),
    cdm_column = c("atc", "atc"),
    code = c("N02", "N02"),
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = 1L
  )

  expect_error(
    apply_codelist(con, codelist, materialize = "in_database"),
    "Tables not found in schema 'main': MISSING"
  )
  expect_false(DBI::dbExistsTable(con, "concept_table"))
})

test_that("apply_codelist refreshes payload fields on conflict", {
  con <- create_medicines_test_db()
  codelist <- data.table(
    id_set = 1L,
    concept_id = "C1",
    cdm_table_name = "MED",
    cdm_column = "atc",
    code = "N02",
    keep_value_column_name = "lvl",
    keep_date_column_name = "d",
    order_index = 1L
  )

  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )
  DBI::dbExecute(
    con,
    "UPDATE MED SET d = DATE '2021-02-03' WHERE atc = 'N02'"
  )
  codelist[, `:=`(id_set = 2L, keep_value_column_name = "sys")]

  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )

  result <- DBI::dbGetQuery(
    con,
    paste(
      "SELECT value, date, id_set FROM concept_table",
      "ORDER BY person_id"
    )
  )
  expect_equal(result$value, c("ATC", "ATC"))
  expect_equal(result$date, as.Date(rep("2021-02-03", 2)))
  expect_equal(result$id_set, c("2", "2"))
})

test_that("apply_codelist resolves a source table in the supplied schema", {
  con <- create_medicines_test_db()
  DBI::dbExecute(con, "CREATE SCHEMA cdm")
  DBI::dbExecute(con, "CREATE TABLE cdm.MED AS SELECT * FROM MED")

  codelist <- data.table(
    id_set = 1L,
    concept_id = "C1",
    cdm_table_name = "MED",
    cdm_column = "atc",
    code = "N02",
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = 1L
  )

  suppressMessages(
    apply_codelist(
      con,
      codelist,
      scheme = "cdm",
      materialize = "in_database"
    )
  )

  result <- dbGetQuery(con, "SELECT person_id FROM concept_table")
  expect_setequal(result$person_id, c("p1", "p3"))
})

test_that("apply_codelist omits id_set when keep_id_set is FALSE", {
  con <- create_medicines_test_db()
  codelist <- data.table(
    id_set = 1L,
    concept_id = "C1",
    cdm_table_name = "MED",
    cdm_column = "atc",
    code = "N02",
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = 1L
  )

  suppressMessages(
    apply_codelist(
      con,
      codelist,
      materialize = "in_database",
      keep_id_set = FALSE
    )
  )

  expect_false("id_set" %in% dbListFields(con, "concept_table"))
  expect_setequal(
    dbGetQuery(con, "SELECT person_id FROM concept_table")$person_id,
    c("p1", "p3")
  )
})

test_that("apply_codelist keeps parents on different columns of one table", {
  con <- create_medicines_test_db()

  # Two id_sets on the same table and keep columns (same family), but
  # matching on different columns: atc = N02 and prod = P1
  codelist <- data.table(
    id_set = c(1L, 2L),
    concept_id = c("C_ATC", "C_PROD"),
    cdm_table_name = "MED",
    cdm_column = c("atc", "prod"),
    code = c("N02", "P1"),
    keep_value_column_name = NA_character_,
    keep_date_column_name = "d",
    order_index = 1L
  )

  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )

  concept_table <- dbGetQuery(
    con,
    "SELECT person_id, concept_id FROM concept_table ORDER BY person_id"
  )
  expect_equal(concept_table$person_id, c("p1", "p2", "p3"))
  expect_equal(concept_table$concept_id, c("C_ATC", "C_PROD", "C_ATC"))
})

test_that("apply_codelist keep_value_column_name is working", {
  con <- create_medicines_test_db()
  
  # Two id_sets on the same table and keep columns (same family), but
  # matching on different columns: atc = N02 and prod = P1
  codelist <- data.table(
    id_set = c(1L, 2L),
    concept_id = c("C_ATC", "C_PROD"),
    cdm_table_name = "MED",
    cdm_column = c("atc", "prod"),
    code = c("N02", "P1"),
    keep_value_column_name = c("lvl",""),
    keep_date_column_name = "d",
    order_index = 1L
  )
  
  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )
  
  concept_table <- dbGetQuery(
    con,
    "SELECT * FROM concept_table ORDER BY person_id"
  )
  expect_equal(concept_table$person_id, c("p1", "p2", "p3"))
  expect_equal(concept_table$concept_id, c("C_ATC", "C_PROD", "C_ATC"))
  expect_equal(concept_table$value, c("A", "TRUE", "B"))
})

# p1: N02 / ATC / level A, p2: product P1, p3: N02 / ATC / level B
create_medicines_test_db_2 <- function(env = parent.frame()) {
  con <- DBI::dbConnect(duckdb::duckdb())
  withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), envir = env)
  DBI::dbExecute(con, "
    CREATE TABLE MED (
      unique_id UUID,
      ori_table VARCHAR,
      person_id VARCHAR,
      atc VARCHAR,
      prod VARCHAR,
      sys VARCHAR,
      lvl VARCHAR,
      d DATE
    )")
  DBI::dbExecute(con, "
    INSERT INTO MED VALUES
      ('00000000-0000-0000-0000-000000000001', 'MED', 'p1', 'N02', 'X', 'ATC', 'A', '2020-01-01'),
      ('00000000-0000-0000-0000-000000000002', 'MED', 'p2', 'Z99', 'P1', 'ATC', 'A', '2020-01-01'),
      ('00000000-0000-0000-0000-000000000003', 'MED', 'p3', 'N02', 'X', 'NON-ATC', 'B', '2020-01-01')")
  con
}

test_that("apply_codelist identify recordsa with 2 search index", {
  con <- create_medicines_test_db_2()
  
  # Two id_sets on the same table and keep columns (same family), but
  # matching on different columns: atc = N02 and prod = P1
  codelist <- data.table(
    id_set = c(1L, 1L, 2L),
    concept_id = c("C_ATC", "C_ATC","C_PROD"),
    cdm_table_name = "MED",
    cdm_column = c("atc", "sys","prod"),
    code = c("N02","ATC","P1"),
    keep_value_column_name = "lvl",
    keep_date_column_name = "d",
    order_index = c(1L,2L,1L)
  )
  
  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )
  
  concept_table <- dbGetQuery(
    con,
    "SELECT * FROM concept_table ORDER BY person_id"
  )
  expect_equal(concept_table$person_id, c("p1", "p2"))
  expect_equal(concept_table$concept_id, c("C_ATC", "C_PROD"))
  expect_equal(concept_table$value, c("A", "A"))
})

test_that("apply_codelist identify recordsa with 2 search index where records are not identified", {
  con <- create_medicines_test_db_2()
  
  # Two id_sets on the same table and keep columns (same family), but
  # matching on different columns: atc = N02 and prod = P1
  codelist <- data.table(
    id_set = c(1L, 1L, 2L),
    concept_id = c("C_ATC", "C_ATC","C_PROD"),
    cdm_table_name = "MED",
    cdm_column = c("atc", "sys","prod"),
    code = c("N02","A","P1"), #code = A does not exist and p1 is not identified, opposed to previous case
    keep_value_column_name = "lvl",
    keep_date_column_name = "d",
    order_index = c(1L,2L,1L)
  )
  
  suppressMessages(
    apply_codelist(con, codelist, materialize = "in_database")
  )
  
  concept_table <- dbGetQuery(
    con,
    "SELECT * FROM concept_table ORDER BY person_id"
  )
  expect_equal(concept_table$person_id, c("p2"))
  expect_equal(concept_table$concept_id, c("C_PROD"))
  expect_equal(concept_table$value, c("A"))
})
