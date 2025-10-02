################################################################################
################################### SETUP ######################################
################################################################################

# Define reusable parameters
dm <- "conception"
format_c <- "csv"
format_p <- "parquet"
siv <- "individual_views"
sc <- "conception"
scv <- "combined_views"
ver <- 1

# Define testing data paths
dbpath <- tempfile(fileext = ".duckdb")
dbdir <- "dbtest/"
dbdir_parquet <- "dbtest_parquet/"
eptcs <- "dbtest/ConcePTION_CDM tables v2.2.xlsx"
tables <- c("PERSONS", "VACCINES", "MEDICINES", "EVENTS")

################################################################################
################################## load_db #####################################
################################################################################

test_that("load_db runs end-to-end with all possible combinations of inputs", {
  for (tp in c("yes", "no")) {
    for (cdb in c("views", "tables")) {
      if (tp == "no" && cdb == "views") {
        expect_warning(
          load_db(
            data_model = dm,
            excel_path_to_cdm_schema = eptcs,
            format_source_files = format_c,
            folder_path_to_source_files = dbdir,
            through_parquet = "no",
            file_path_to_target_db = temp_db,
            create_db_as = "views",
            verbosity = ver
          ),
          regexp = "Invalid parameter combination"
        )
      } else {
        expect_output(
          load_db(
            data_model = dm,
            excel_path_to_cdm_schema = eptcs,
            format_source_files = format_c,
            folder_path_to_source_files = dbdir,
            through_parquet = tp,
            file_path_to_target_db = temp_db,
            create_db_as = cdb,
            verbosity = ver
          ),
          regexp = "Hooray! Script finished running!"
        )
      }
    }
  }

  # Check that database connection is closed
  expect_false(DBI::dbIsValid(con))
})