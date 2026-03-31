# Load of packages needed for the T2 Transformation step
install_package_fun <- function(name_fun) {
  package_name <- as.character(name_fun)
  if (!require(package_name, character.only = TRUE)) {
    install.packages(package_name)
  }
  suppressPackageStartupMessages(library(package_name, character.only = TRUE))
}

list_of_packages <- c(
  "stringr",
  "data.table",
  "rlist",
  "DBI",
  "sqldf",
  "duckdb",
  "tidyverse"
)
lapply(list_of_packages, function(package_name) {
  install_package_fun(package_name)
})
