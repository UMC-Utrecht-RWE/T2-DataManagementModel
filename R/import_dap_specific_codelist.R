#' Import DAP-Specific Codelist to Database
#'
#' @description 
#' Reads a codelist, filters for active rows ("BOTH"), converts specified 
#' columns to uppercase, and writes the result to a database.
#' 
#' @param codelist_path String. Path to the .rds file.
#' @param codelist_name_db String. Target table name in the database.
#' @param db_connection DBI connection object.
#' @param columns Character vector. Columns to keep and transform.
#'
#' @export
#' Adaptive Import of DAP-Specific Codelist

import_dap_specific_codelist <- function(codelist_path, codelist_name_db,
                                         db_connection, columns) {
  
  if (!file.exists(codelist_path)) stop("File not found.")
  
  # 1. Determine the file extension
  ext <- tolower(tools::file_ext(codelist_path))
  
  # 2. Adaptive Reading Logic
  codelist <- switch(ext,
                     "rds"  = readRDS(codelist_path),
                     "csv"  = data.table::fread(codelist_path, ...),
                     "xlsx" = {
                       if (!requireNamespace("readxl", quietly = TRUE)) 
                         stop("Package 'readxl' needed for .xlsx files.")
                       readxl::read_xlsx(codelist_path, ...)
                     },
                     stop("Unsupported file extension: .", ext)
  )
  
  # 3. Standardize to data.table
  # Using the T2.DMM internal or data.table directly
  codelist <- data.table::as.data.table(codelist)
  
  # 4. Validation (Ensure columns exist)
  required_cols <- unique(c(columns, "Comment"))
  if (!all(required_cols %in% names(codelist))) {
    stop("Missing columns. Found: ", paste(names(codelist), collapse = ", "))
  }
  
  # 5. Filter & Process (Logic remains the same)
  # Filter for BOTH (case-insensitive)
  codelist <- unique(codelist[toupper(Comment) == "BOTH", ..columns])
  
  # In-place uppercase conversion
  for (col in columns) {
    set(codelist, j = col, value = toupper(as.character(codelist[[col]])))
  }
  
  # 6. Database Write
  DBI::dbWriteTable(db_connection, codelist_name_db, codelist, overwrite = TRUE)
  
  message(sprintf("Imported %d rows from .%s file.", nrow(codelist), ext))
}