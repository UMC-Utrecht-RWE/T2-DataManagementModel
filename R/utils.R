#' Read SQL script from file and return as a single string
#' Function to make sure we read SQL scripts properly
#' (replace -- to /* .... */ top prevent line end errors)
#' @param filepath The path to the SQL file
#' @return A single string containing the SQL script
#' @export
getSQL <- function(filepath) { #nolint
  con <- file(filepath, "r")
  sql_string <- ""

  while (TRUE) {
    line <- readLines(con, n = 1, encoding = "UTF-16")

    if (length(line) == 0) {
      break
    }

    line <- gsub("\\t", " ", line)

    if (grepl("--", line) == TRUE) {
      line <- paste(sub("--", "/*", line), "*/")
    }

    sql_string <- paste(sql_string, line)

  }

  close(con)
  sql_string
}
