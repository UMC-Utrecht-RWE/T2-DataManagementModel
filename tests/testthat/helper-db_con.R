create_loaded_test_db <- function(csv_dir = "dbtest",
                                  tables = c("PERSONS", "VACCINES"),
                                  metadata = concePTION_metadata_v2) {
  dbname <- tempfile("ConcePTION.duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  schema <- T2.DMM:::load_db(
    con = con,
    excel_path_to_cdm_schema = "dbtest/ConcePTION_CDM tables v2.2.xlsx",
    format_source_files = "csv",
    folder_path_to_source_files = csv_dir,
    tables_in_cdm = tables
  )

  return(list(con = con, schema = schema))
}

create_database_loader <- function(config_path) {
  DatabaseLoader$new(
    db_path = "",
    data_instance = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    config_path = Sys.getenv(config_path)
  )
}

create_loader_from_file <- function(config_path) {
  DatabaseLoader$new(
    db_path = "",
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
    db_path = "",
    data_instance = "dbtest",
    cdm_metadata = file.path(
      getwd(), "dbtest", "CDM_metadata.rds"
    ),
    config_path = Sys.getenv(config_path)
  )
}