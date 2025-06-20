% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/load_db.R
\name{load_db}
\alias{load_db}
\title{Load CDM Data Set Instance from CSV Files into a Database}
\usage{
load_db(
  db_connection,
  data_instance_path,
  cdm_metadata,
  cdm_tables_names,
  extension_name = ""
)
}
\arguments{
\item{db_connection}{A database connection object (SQLiteConnection).}

\item{data_instance_path}{Path to the CSV CDM database.}

\item{cdm_metadata}{Data.table with Table name, Variable name, and Format.}

\item{cdm_tables_names}{List of CDM tables names to be imported into the DB.}

\item{extension_name}{String to be added to the name of the tables, useful
when loading different CDM instances in the same DB.}
}
\description{
This function is designed for loading a CDM data set instance from CSV files
into a database.
}
\keyword{internal}
