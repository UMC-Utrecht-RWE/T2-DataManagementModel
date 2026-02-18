create_codelist_example <- function() {
  data.table::data.table(
    concept_id = "WEIGHT_BIRTH",
    dap_name = "CPRD",
    cdm_name = "CONCEPTION",
    cdm_table_name = "MEDICAL_OBSERVATIONS",
    column_name_1 = "mo_meaning",
    column_name_2 = NA,
    column_name_3 = NA,
    expected_value_1 = "weight",
    expected_value_2 = NA,
    expected_value_3 = NA,
    keep_value_column_name = "mo_source_value",
    keep_unit_column_name = NA,
    keep_date_column_name = "mo_date",
    IR1 = NA,
    Comments = NA,
    dap_spec_id = "dap_spec_id-1"
  )
}

create_test_db <- function(source_db_path, source_db_conn, concept_db_conn, attach_name) {
  mo <- readRDS(testthat::test_path("dbtest", "MEDICAL_OBSERVATIONS.rds"))
  mo <- as.data.frame(mo)
  DBI::dbWriteTable(
    source_db_conn,
    "MEDICAL_OBSERVATIONS",
    mo,
    overwrite = TRUE
  )

  T2.DMM:::create_unique_id(
    source_db_conn,
    cdm_tables_names = "MEDICAL_OBSERVATIONS",
    to_view = FALSE
  )
  DBI::dbDisconnect(source_db_conn)


  DBI::dbExecute(
    concept_db_conn,
    "CREATE TABLE concept_table (
      ori_table TEXT,
      unique_id UUID,
      person_id TEXT,
      code TEXT,
      coding_system TEXT,
      value TEXT,
      concept_id TEXT,
      date DATE,
      meaning TEXT
    );"
  )

  DBI::dbExecute(
    concept_db_conn,
    paste0("ATTACH DATABASE '", source_db_path, "' AS ", attach_name)
  )
  
}

cleanup_concept_tables <- function(db_connection) {
  if ("MEDICAL_OBSERVATIONS_EDITED_dapspec" %in%
        DBI::dbListTables(db_connection)) {
    DBI::dbExecute(
      db_connection, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
    )
  }
  DBI::dbExecute(db_connection, "DELETE FROM concept_table")
}

testthat::test_that("retrieve MEDICAL_OBSERVATIONS concepts", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  
  withr::defer(DBI::dbDisconnect(concept_db_conn), envir = parent.frame())
  withr::defer(cleanup_concept_tables(concept_db_conn), envir = parent.frame())

  create_dap_specific_concept(
    codelist = create_codelist_example(),
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )

  mo_concept_table <- DBI::dbReadTable(concept_db_conn, "concept_table")
  testthat::expect_equal(nrow(mo_concept_table), 39)
})

testthat::test_that("Error messages", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  
  testthat::expect_error(
    create_dap_specific_concept(
      codelist = data.table::data.table(),
      name_attachment = attach_name,
      save_db = concept_db_conn,
      intermediate_type = "something.something"
    ),
    "Codelist does not contain any data."
  )

  testthat::expect_error(
    create_dap_specific_concept(
      codelist = create_codelist_example(),
      name_attachment = attach_name,
      save_db = concept_db_conn,
      intermediate_type = "something.something"
    ),
    "intermediate_type has to be either TABLE or VIEW."
  )
})


testthat::test_that("existing MEDICAL_OBSERVATIONS_EDITED is handled", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn), envir = parent.frame())
  withr::defer(cleanup_concept_tables(concept_db_conn), envir = parent.frame())

  DBI::dbWriteTable(
    concept_db_conn,
    "MEDICAL_OBSERVATIONS_EDITED",
    data.frame(test = 1),
    overwrite = TRUE
  )

  create_dap_specific_concept(
    codelist = create_codelist_example(),
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )

  testthat::expect_true(
    "MEDICAL_OBSERVATIONS_EDITED" %in% DBI::dbListTables(concept_db_conn)
  )
})

testthat::test_that("reference non-existent column errors", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn), envir = parent.frame())
  withr::defer(cleanup_concept_tables(concept_db_conn), envir = parent.frame())

  codelist <- create_codelist_example()[
    , `:=`(column_name_1 = "something", expected_value_1 = "anything")
  ]

  testthat::expect_error(
    create_dap_specific_concept(
      codelist = codelist,
      name_attachment = attach_name,
      save_db = concept_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE
    )
  )
})

testthat::test_that("NA or missing keep_value_column_name yields TRUE", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn), envir = parent.frame())
  withr::defer(cleanup_concept_tables(concept_db_conn), envir = parent.frame())

  codelist_na_keep_value <- create_codelist_example()[
    , keep_value_column_name := NA_character_
  ]

  create_dap_specific_concept(
    codelist = codelist_na_keep_value,
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )

  mo_concept_table <- DBI::dbReadTable(concept_db_conn, "concept_table")
  testthat::expect_true(all(mo_concept_table$value == "true"))

  DBI::dbExecute(concept_db_conn, "DELETE FROM concept_table")
  DBI::dbExecute(
    concept_db_conn, "DROP TABLE MEDICAL_OBSERVATIONS_EDITED_dapspec"
  )

  codelist_no_keep_value <- create_codelist_example()[
    , keep_value_column_name := NULL
  ]

  create_dap_specific_concept(
    codelist = codelist_no_keep_value,
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE
  )

  mo_concept_table <- DBI::dbReadTable(concept_db_conn, "concept_table")
  testthat::expect_true(all(mo_concept_table$value == "true"))
})


testthat::test_that("save_in_parquet FALSE with or without partition_var", {
  local({
    source_db_path <- tempfile(fileext = ".duckdb")
    source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
    
    concept_db_conn <- DBI::dbConnect(
      duckdb::duckdb(), tempfile(fileext = ".duckdb")
    )
    
    attach_name <- "d2_db_conn"
    
    create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
    withr::defer(DBI::dbDisconnect(concept_db_conn))
    withr::defer(cleanup_concept_tables(concept_db_conn))

    create_dap_specific_concept(
      codelist = create_codelist_example(),
      name_attachment = attach_name,
      save_db = concept_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE,
      save_in_parquet = FALSE,
      partition_var = "concept_id"
    )

    mo_concept_table <- DBI::dbReadTable(concept_db_conn, "concept_table")
    testthat::expect_equal(nrow(mo_concept_table), 39)
  })

  local({
    source_db_path <- tempfile(fileext = ".duckdb")
    source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
    
    concept_db_conn <- DBI::dbConnect(
      duckdb::duckdb(), tempfile(fileext = ".duckdb")
    )
    
    attach_name <- "d2_db_conn"
    
    create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
    withr::defer(DBI::dbDisconnect(concept_db_conn))
    withr::defer(cleanup_concept_tables(concept_db_conn))

    create_dap_specific_concept(
      codelist = create_codelist_example(),
      name_attachment = attach_name,
      save_db = concept_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE,
      save_in_parquet = FALSE,
      partition_var = NULL
    )

    mo_concept_table <- DBI::dbReadTable(concept_db_conn, "concept_table")
    testthat::expect_equal(nrow(mo_concept_table), 39)
  })
})



testthat::test_that("save_in_parquet TRUE with partitioning", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn))
  withr::defer(cleanup_concept_tables(concept_db_conn))

  partitioned_dir <- tempfile(pattern = "dap_partitioned_")
  dir.create(partitioned_dir)
  withr::defer(unlink(partitioned_dir, recursive = TRUE))

  create_dap_specific_concept(
    codelist = create_codelist_example(),
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE,
    save_in_parquet = TRUE,
    partition_var = "concept_id",
    dir_save = partitioned_dir
  )

  partitioned_rows <- DBI::dbGetQuery(
    concept_db_conn,
    paste0(
      "SELECT COUNT(*) AS n_rows FROM read_parquet('",
      partitioned_dir, "/*/*.parquet', hive_partitioning = true)"
    )
  )
  testthat::expect_equal(partitioned_rows$n_rows[[1]], 39)
})

testthat::test_that("save_in_parquet TRUE without partitioning", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn))
  withr::defer(cleanup_concept_tables(concept_db_conn))

  output_file <- tempfile(fileext = ".parquet")
  withr::defer(unlink(output_file))

  create_dap_specific_concept(
    codelist = create_codelist_example(),
    name_attachment = attach_name,
    save_db = concept_db_conn,
    date_col_filter = "1900-01-01",
    add_meaning = TRUE,
    save_in_parquet = TRUE,
    partition_var = NULL,
    dir_save = output_file
  )

  non_partitioned_rows <- DBI::dbGetQuery(
    concept_db_conn,
    paste0(
      "SELECT COUNT(*) AS n_rows FROM read_parquet('",
      output_file, "')"
    )
  )
  testthat::expect_equal(non_partitioned_rows$n_rows[[1]], 39)
})

testthat::test_that("prints 'Meaning not identified'", {
  source_db_path <- tempfile(fileext = ".duckdb")
  source_db_conn <- DBI::dbConnect(duckdb::duckdb(), source_db_path)
  
  concept_db_conn <- DBI::dbConnect(
    duckdb::duckdb(), tempfile(fileext = ".duckdb")
  )
  
  attach_name <- "d2_db_conn"
  
  create_test_db(source_db_path, source_db_conn,concept_db_conn, attach_name)
  withr::defer(DBI::dbDisconnect(concept_db_conn))
  withr::defer(cleanup_concept_tables(concept_db_conn))

  # Remove any column containing "meaning"
  DBI::dbExecute(
    concept_db_conn,
    paste0(
      "ALTER TABLE ", attach_name,
      ".MEDICAL_OBSERVATIONS RENAME COLUMN mo_meaning TO mo_label"
    )
  )

  # Use the renamed column for matching
  codelist <- create_codelist_example()[, column_name_1 := "mo_label"]

  testthat::expect_output(
    create_dap_specific_concept(
      codelist = codelist,
      name_attachment = attach_name,
      save_db = concept_db_conn,
      date_col_filter = "1900-01-01",
      add_meaning = TRUE
    ),
    "\\[create_dap_specific_concept\\] Meaning not identified for:"
  )
})
