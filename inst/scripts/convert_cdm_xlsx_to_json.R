# Converts the ConcePTION CDM tables Excel schema into a JSON file.
# Each top-level key is a CDM table name; its value is an array of column
# definitions (Variable, Mandatory, Description, Format, Vocabulary, Comments).
# Sheets without a "Variable" column (documentation sheets) are skipped.

library(openxlsx)
library(jsonlite)
library(dplyr)

args <- commandArgs(trailingOnly = TRUE)
excel_path <- if (length(args) >= 1) args[1] else "inst/extdata/ConcePTION_CDM_tables_v2.2.xlsx"
json_path <- if (length(args) >= 2) args[2] else "inst/extdata/ConcePTION_CDM_tables_v2.2.json"

sheet_names <- getSheetNames(excel_path)

cdm_schema <- list()

for (sheet_name in sheet_names) {
  sheet_data <- tryCatch(
    read.xlsx(excel_path, sheet = sheet_name, startRow = 4),
    error = function(e) NULL
  )

  if (is.null(sheet_data) || !"Variable" %in% names(sheet_data)) {
    next
  }

  sheet_data <- sheet_data %>%
    mutate(Variable = trimws(Variable, whitespace = "[\\h\\v]")) %>%
    filter(!is.na(Variable)) %>%
    filter(!cumany(Variable == "Conventions")) %>%
    select(any_of(c(
      "Variable", "Mandatory", "Description",
      "Format", "Vocabulary", "Comments"
    )))

  cdm_schema[[sheet_name]] <- sheet_data
}

write_json(cdm_schema, json_path, pretty = TRUE, na = "null")

cat("CDM schema written to:", json_path, "\n")
