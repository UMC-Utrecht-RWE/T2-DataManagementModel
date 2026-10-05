## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)


## -----------------------------------------------------------------------------
library(T2.DMM)
library(data.table)
library(DBI)
library(duckdb)

## -----------------------------------------------------------------------------
db_path <- tempfile(pattern = "cdm", fileext = ".duckdb")

## -----------------------------------------------------------------------------
# Initialize the DatabaseLoader
loader <- T2.DMM::DatabaseLoader$new(
  db_path = db_path,
  data_instance = file.path(getwd(), "vignettes/data/"),
  config_path = file.path(getwd(), "vignettes/data/set_db.json"),
  cdm_metadata = file.path(getwd(), "vignettes/data/CDM_metadata.rds")
)

## -----------------------------------------------------------------------------
# Load the data into the database
loader$set_database()

## -----------------------------------------------------------------------------
# Execute all configured operations
loader$run_db_ops()
