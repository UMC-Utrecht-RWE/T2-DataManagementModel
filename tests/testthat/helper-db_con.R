# Copy the dbtest/ fixtures into a temp folder that is deleted when `env`
# exits, so load_db() writes its intermediate_parquet/ outside the repository.
local_dbtest_copy <- function(env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = env)
  fixtures <- list.files(testthat::test_path("dbtest"), full.names = TRUE)
  file.copy(fixtures[!dir.exists(fixtures)], dir)
  dir
}

create_loaded_test_db <- function(csv_dir = local_dbtest_copy(env),
                                  tables = c("PERSONS", "VACCINES"),
                                  metadata = concePTION_metadata_v2,
                                  env = parent.frame()) {
  dbname <- tempfile("test.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  # suppressMessages(
  load_db(
    con = con,
    cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.yaml",
    format_source_files = "csv",
    folder_path_to_source_files = csv_dir,
    tables_in_cdm = tables,
    create_db_as = "view"
  )

  return(con) # nolint
}

create_test_db_materialized <- function(csv_dir = local_dbtest_copy(env),
                                        tables = c("PERSONS", "VACCINES"),
                                        metadata = concePTION_metadata_v2,
                                        env = parent.frame()) {
  dbname <- tempfile("test.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  # suppressMessages(
  load_db(
    con = con,
    cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.yaml",
    format_source_files = "csv",
    folder_path_to_source_files = csv_dir,
    tables_in_cdm = tables,
    create_db_as = "tables"
  )

  return(con) # nolint
}


create_database_loader <- function(config_path, env = parent.frame()) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = local_dbtest_copy(env),
    cdm_metadata = concePTION_metadata_v2,
    config_path = Sys.getenv(config_path)
  )
}

create_loader_from_file <- function(config_path, env = parent.frame()) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = local_dbtest_copy(env),
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.rds"
    ),
    config_path = Sys.getenv(config_path)
  )
}

create_loader_from_wrong_file <- function(config_path) {
  DatabaseLoader$new(
    db_path = NULL,
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.csv"
    ),
    config_path = Sys.getenv(config_path)
  )
}

create_loader_bad_path <- function(config_path, env = parent.frame()) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = local_dbtest_copy(env),
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.rds"
    ),
    config_path = Sys.getenv(config_path)
  )
}
