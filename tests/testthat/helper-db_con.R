create_loaded_test_db <- function(csv_dir = "dbtest",
                                  tables = c("PERSONS", "VACCINES"),
                                  metadata = concePTION_metadata_v2) {
  dbname <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  T2.DMM:::load_db(
    db_connection = con,
    data_instance_path = csv_dir,
    cdm_metadata = metadata,
    cdm_tables_names = tables
  )

  return(con)
}

create_database_loader <- function(config_path) {
  DatabaseLoader$new(
    db_path = "",
    data_instance = "dbtest",
    cdm_metadata = concePTION_metadata_v2,
    config_path = Sys.getenv(config_path)
  )
}