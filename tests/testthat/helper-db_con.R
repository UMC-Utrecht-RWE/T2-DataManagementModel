create_loaded_test_db <- function(csv_dir = "dbtest/",
                                  tables = c("PERSONS", "VACCINES"),
                                  metadata = concePTION_metadata_v2) {
  dbname <- tempfile("test.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  # suppressMessages(
  load_db(
    con = con,
    cdm_schema = "dbtest/ConcePTION_CDM_tables_v2.2.json",
    format_source_files = "csv",
    folder_path_to_source_files = csv_dir,
    tables_in_cdm = tables,
    create_db_as = "view"
  ) # )

  return(con)
}

create_database_loader <- function(config_path) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    config_path = Sys.getenv(config_path)
  )
}

create_loader_from_file <- function(config_path) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = "dbtest",
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

create_loader_bad_path <- function(config_path) {
  DatabaseLoader$new(
    db_path = tempfile("test.duckdb"),
    data_instance = "dbtest",
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.rds"
    ),
    config_path = Sys.getenv(config_path)
  )
}
