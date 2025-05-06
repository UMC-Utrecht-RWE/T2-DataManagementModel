create_loaded_test_db <- function(csv_dir = "dbtest",
                                  tables = c("PERSONS", "VACCINES"),
                                  metadata = concePTION_metadata_v2) {
  dbname <- tempfile(fileext = ".duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), dbname)

  T2.DMM::load_db(
    db_connection = con,
    csv_path_dir = csv_dir,
    cdm_metadata = metadata,
    cdm_tables_names = tables
  )

  return(con)
}
