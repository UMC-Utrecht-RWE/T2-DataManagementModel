##############################################################
#### Function to make sure we read SQL scripts properly     ##
#### (replace -- to /* .... */ top prevent line end errors) ##
##############################################################
getSQL <- function(filepath) {
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