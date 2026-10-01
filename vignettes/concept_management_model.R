## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
    collapse = TRUE,
    comment = "#>"
)

## ----eval=FALSE---------------------------------------------------------------
# # Install the package (example installation method)
# devtools::install_github("UMC-Utrecht-RWE/T2-DataManagementModel")

## -----------------------------------------------------------------------------
library(T2.DMM)
library(data.table)
library(DBI)
library(duckdb)

## -----------------------------------------------------------------------------
db_path <- tempfile(pattern = "cdm", fileext = ".duckdb")

## -----------------------------------------------------------------------------
# Initialize the DatabaseLoader
loader <- DatabaseLoader$new(
    db_path = db_path,
    data_instance = file.path(getwd(), "data/"),
    config_path = file.path(getwd(), "data/set_db.json"),
    cdm_metadata = file.path(getwd(), "data/CDM_metadata.rds")
)

## -----------------------------------------------------------------------------
# Load the data into the database
loader$set_database()

