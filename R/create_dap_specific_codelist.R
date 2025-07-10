#' Create DAP-Specific Codelist
#'
#' This function merges the unique codelist from a database
#' (you can use the function getUniqueCodeList) with a study code list.
#'
#' @param unique_codelist Data.table containing unique code list output
#' from the function getUniqueCodeList.
#' @param study_codelist Data.table containing the study code list.
#' @param start_with_colls Columns to start with
#' @param additional_columns Additional columns to be included in the result.
#' @param priority Priority column for selecting codes when there are
#' multiple matches.
#'
#' @return A merged data.table containing DAP-specific codes,
#' including exact matches and those starting with.
#'
#' @examples
#' \dontrun{
#' # Example usage:
#' result <- create_dap_specific_codelist(unique_codelist, study_codelist,
#'   additional_columns = NA, priority = NA
#' )
#' }
#'
#' @export
create_dap_specific_codelist <- function(
    unique_codelist,
    study_codelist,
    start_with_colls = c(
      "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
      "ICPC", "ICPC2P", "ICPC2EENG", "ATC", "vx_atc"
    ),
    additional_columns = NA,
    priority = NA) {

  # Ensure entries are data.table
  unique_codelist <- T2.DMM:::ensure_data_table(unique_codelist)
  study_codelist <- T2.DMM:::ensure_data_table(study_codelist)

  # Preprocessing the study_codelist
  study_codelist[, code_no_dot := gsub("\\.", "", code)]
  study_codelist[, length_str := stringr::str_length(code_no_dot)]
  data.table::setnames(study_codelist, "code", "code.CDM_CODELIST")
  min_length_study_codelist <- min(study_codelist$length_str, na.rm = TRUE)

  # Dividing code lists into exact and start with codes
  start_unique_codelist <- unique_codelist[coding_system %in% start_with_colls]
  start_study_codelist <- study_codelist[coding_system %in% start_with_colls]
  exact_unique_codelist <- unique_codelist[!coding_system %in% start_with_colls]
  exact_study_codelist <- study_codelist[!coding_system %in% start_with_colls]

  # Finding exact matches
  exact_match <- NULL
  if (nrow(exact_unique_codelist) > 0) {
    exact_match <- merge(exact_unique_codelist, exact_study_codelist,
      by = c("coding_system", "code_no_dot")
    )
  }


  if (nrow(start_unique_codelist) > 0) {
    # Finding start with matches
    # TODO check merge is intersect join; result is a smaller set
    start_exact_match <- merge(start_unique_codelist, start_study_codelist,
      by = c("coding_system", "code_no_dot")
    )

    # Preprocessing start with codes
    start_unique_codelist[, ori_length_str := stringr::str_length(code_no_dot)]
    max_code_length <- max(start_unique_codelist$ori_length_str)
    message(paste0(
      "[SetCodesheets] Max length of code from the DAP is : ",
      max_code_length
    ))

    # Splitting start with codes into different lengths
    list_cols_names <- seq(min_length_study_codelist, max_code_length)
    invisible(lapply(list_cols_names, function(x) {
      start_unique_codelist[ori_length_str >= x, as.character(x) :=
        substr(code_no_dot, 1, as.numeric(x))]
    }))

    # Melting the dataset
    start_unique_codelist <- data.table::melt(start_unique_codelist,
      variable.name = "length_str",
      measure.vars = as.character(list_cols_names), na.rm = TRUE,
      variable.factor = FALSE,
      value.name = "code_no_dot2"
    )

    start_unique_codelist <- start_unique_codelist[
      ,
      length_str := as.numeric(length_str)
    ]

    # Inner joining start with codes with the study codelist
    results_startwith <- merge(
      x = start_unique_codelist,
      y = start_study_codelist,
      by.x = c("coding_system", "code_no_dot2", "length_str"),
      by.y = c("coding_system", "code_no_dot", "length_str")
    )

    # Selecting the top priority matches
    cols_by <- c("code.DAP_UNIQUE_CODELIST", "concept_id", "coding_system")
    if (!all(is.na(additional_columns))) {
      cols_by <- c(cols_by, additional_columns)
    }
    if (!is.na(priority)) {
      results_startwith2 <- data.table::copy(results_startwith)[
        order(
          -length_str,
          eval(priority)
        ),
        .SD[1],
        by = cols_by
      ]
    } else {
      results_startwith2 <- data.table::copy(results_startwith)[
        order(-length_str),
        .SD[1],
        by = cols_by
      ]
    }
  }

  # Compile all results into one output
  dap_specific_codelist <- exact_match

  if (nrow(start_unique_codelist) > 0) {
    cols_sel <- names(start_exact_match)
    dap_specific_codelist <- data.table::rbindlist(
      list(
        dap_specific_codelist,
        results_startwith2[, ..cols_sel]
      ),
      use.names = TRUE
    )
  }

  # Finding missing codes
  missing_from_cdm <- data.table::as.data.table(
    dplyr::anti_join(
      unique_codelist,
      dap_specific_codelist,
      by = c("coding_system", "code_no_dot")
    )
  )
  missing_from_codelist <- data.table::as.data.table(
    dplyr::anti_join(
      study_codelist,
      dap_specific_codelist,
      by = c("coding_system", "code_no_dot")
    )
  )

  # Combining all results
  dap_specific_codelist <- data.table::rbindlist(
    list(
      dap_specific_codelist,
      missing_from_cdm,
      missing_from_codelist
    ),
    fill = TRUE
  )

  # Adding Comment column
  dap_specific_codelist[, Comment := "BOTH"]
  dap_specific_codelist[is.na(code.DAP_UNIQUE_CODELIST), Comment := "CODELIST"]
  dap_specific_codelist[is.na(code.CDM_CODELIST), Comment := "CDM"]

  return(dap_specific_codelist)
}
