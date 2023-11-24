#' Create DAP-Specific Codelist
#'
#' This function merges the unique codelist from a database (you can use the function getUniqueCodeList) with a study code list.
#'
#' @param UNIQUE_CODELIST Data.table containing unique code list output from the function getUniqueCodeList.
#' @param STUDY_CODELIST Data.table containing the study code list.
#' @param additionalColumns Additional columns to be included in the result.
#' @param priority Priority column for selecting codes when there are multiple matches.
#'
#' @return A merged data.table containing DAP-specific codes, including exact matches and those starting with.
#'
#' @author Albert Cid Royo
#' @email a.cidroyo@umcutrecht.nl
#' @organisation UMC Utrecht, Utrecht, The Netherlands
#' @date 01/12/2022
#'
#' @importFrom data.table data.table melt copy rbindlist anti_join str_length gsub setnames
#' @importFrom base order
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' result <- CREATE_DAP_SPECIFIC_CODELIST(UNIQUE_CODELIST, STUDY_CODELIST, additionalColumns = NA, priority = NA)
#' }
#'
#' @export
#'

CREATE_DAP_SPECIFIC_CODELIST <- function(UNIQUE_CODELIST, STUDY_CODELIST, additionalColumns = NA, priority = NA) {
  # Preprocessing the STUDY_CODELIST
  STUDY_CODELIST[, code_no_dot := gsub("\\.", "", code)]
  STUDY_CODELIST[, length_str := str_length(code_no_dot)]
  setnames(STUDY_CODELIST, "code", "code.CDM_CODELIST")
  minLength_study_CODELIST <- min(STUDY_CODELIST$length_str, na.rm = TRUE)

  # Dividing code lists into exact and start with codes
  start_UNIQUE_CODELIST <- UNIQUE_CODELIST[coding_system %in% start_with_colls]
  start_study_CODELIST <- STUDY_CODELIST[coding_system %in% start_with_colls]
  exact_UNIQUE_CODELIST <- UNIQUE_CODELIST[!coding_system %in% start_with_colls]
  exact_study_CODELIST <- STUDY_CODELIST[!coding_system %in% start_with_colls]

  # Finding exact matches
  exactMatch <- merge(exact_UNIQUE_CODELIST, exact_study_CODELIST, by = c("coding_system", "code_no_dot"))

  # Finding start with matches
  start_exactMatch <- merge(start_UNIQUE_CODELIST, start_study_CODELIST, by = c("coding_system", "code_no_dot"))

  if (nrow(start_UNIQUE_CODELIST) > 0) {
    # Preprocessing start with codes
    start_UNIQUE_CODELIST[, ori_length_str := str_length(code_no_dot)]
    max_code_length <- max(start_UNIQUE_CODELIST$ori_length_str)
    print(paste0("[SetCodesheets] Max length of code from the DAP is : ", max_code_length))

    # Splitting start with codes into different lengths
    listColsNames <- seq(minLength_study_CODELIST, max_code_length)
    invisible(lapply(listColsNames, function(x) {
      start_UNIQUE_CODELIST[ori_length_str >= x, as.character(x) := substr(code_no_dot, 1, as.numeric(x))]
    }))

    # Melting the dataset
    start_UNIQUE_CODELIST <- data.table::melt(start_UNIQUE_CODELIST,
      variable.name = "length_str",
      measure.vars = as.character(listColsNames), na.rm = TRUE,
      variable.factor = FALSE,
      value.name = "code_no_dot2"
    )

    start_UNIQUE_CODELIST <- start_UNIQUE_CODELIST[, length_str := as.numeric(length_str)]

    # Inner joining start with codes with the study codelist
    results_startwith <- merge(
      x = start_UNIQUE_CODELIST,
      y = start_study_CODELIST,
      by.x = c("coding_system", "code_no_dot2", "length_str"),
      by.y = c("coding_system", "code_no_dot", "length_str")
    )

    # Selecting the top priority matches
    colsBy <- c("code.DAP_UNIQUE_CODELIST", "Outcome", "coding_system")
    if (!all(is.na(additionalColumns))) {
      colsBy <- c(colsBy, additionalColumns)
    }
    if (!is.na(priority)) {
      results_startwith2 <- copy(results_startwith)[order(-length_str, eval(priority)), .SD[1], by = colsBy]
    } else {
      results_startwith2 <- copy(results_startwith)[order(-length_str), .SD[1], by = colsBy]
    }
  }

  # Compile all results into one output
  DAP_specific_codelist <- exactMatch

  if (nrow(start_UNIQUE_CODELIST) > 0) {
    cols_sel <- names(DAP_specific_codelist)
    DAP_specific_codelist <- rbindlist(list(DAP_specific_codelist, results_startwith2[, ..cols_sel]), use.names = TRUE)
  }

  # Finding missing codes
  MissingFromCDM <- as.data.table(anti_join(UNIQUE_CODELIST, DAP_specific_codelist, by = c("coding_system", "code_no_dot")))
  MissingFromCODELIST <- as.data.table(anti_join(STUDY_CODELIST, DAP_specific_codelist, by = c("coding_system", "code_no_dot")))

  # Combining all results
  DAP_specific_codelist <- rbindlist(list(DAP_specific_codelist, MissingFromCDM, MissingFromCODELIST), fill = TRUE)

  # Adding Comment column
  DAP_specific_codelist[, Comment := "BOTH"]
  DAP_specific_codelist[is.na(code.DAP_UNIQUE_CODELIST), Comment := "CODELIST"]
  DAP_specific_codelist[is.na(code.CDM_CODELIST), Comment := "CDM"]

  return(DAP_specific_codelist)
}
