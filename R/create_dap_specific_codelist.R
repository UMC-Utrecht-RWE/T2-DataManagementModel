library(dplyr)
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
  start_with_cols = c( #TODO: Should that be cols?
    "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
    "ICPC", "ICPC2P", "ICPC2EENG", "ATC", "vx_atc"
  ),
  additional_columns = NA,
  priority = NA
) {
  # get error if unique_codelist and study_codelist are not data.tables
  if (!is.data.table(unique_codelist))
    stop("unique_codelist must be a data.table")
  if (!is.data.table(study_codelist))
    stop("study_codelist must be a data.table")

  # Preprocessing the study_codelist
  study_codelist <- study_codelist %>%
    dplyr::mutate(code_no_dot = gsub("\\.", "", .data$code)) %>%
    dplyr::mutate(
      length_str = stringr::str_length(.data$code_no_dot)
    ) %>%
    dplyr::mutate(code.CDM_CODELIST = .data$code)

  min_length_study_codelist <- min(study_codelist$length_str, na.rm = TRUE)
  # Dividing code lists into exact matches and start with codes
  # The names refer to further analysis.
  # Here, we get the rows that match start_with_cols and not.
  start_unique_codelist <- unique_codelist %>%
    dplyr::filter(unique_codelist$coding_system %in% start_with_cols)
  start_study_codelist <- study_codelist %>%
    dplyr::filter(study_codelist$coding_system %in% start_with_cols)

  # Perfect match
  exact_unique_codelist <- unique_codelist %>%
    dplyr::filter(!unique_codelist$coding_system %in% start_with_cols)
  exact_study_codelist <- study_codelist %>%
    dplyr::filter(!study_codelist$coding_system %in% start_with_cols)

  print(c( #TODO: It should be logged.
    dim(start_unique_codelist),
    dim(start_study_codelist),
    dim(exact_unique_codelist),
    dim(exact_study_codelist)
  ))

  # Finding exact matches
  exact_match <- exact_unique_codelist %>%
    dplyr::inner_join(
      exact_study_codelist,
      by = c("coding_system", "code_no_dot")
    )

  if (nrow(exact_match) == 0) {
    exact_match <- NULL
  }

  if (nrow(start_unique_codelist) > 0) {
    # Finding start with matches
    start_exact_match <- start_unique_codelist %>%
      dplyr::inner_join(
        start_study_codelist,
        by = c("coding_system", "code_no_dot")
      )

    # Preprocessing start with codes
    start_unique_codelist <- start_unique_codelist %>%
      dplyr::mutate(ori_length_str = stringr::str_length(.data$code_no_dot))

    max_code_length <- max(start_unique_codelist$ori_length_str)

    print(paste0(
      "[SetCodesheets] Max length of code from the DAP is : ",
      max_code_length
    ))

    # Splitting start with codes into different lengths
    list_cols_names <- seq(min_length_study_codelist, max_code_length)
    # Create a new column for each value of list
    # cols_names as increasing window of code.DAP_UNIQUE_CODELIST
    for (x in list_cols_names) {
      rows <- start_unique_codelist$ori_length_str >= x
      start_unique_codelist[
        rows, (as.character(x)) := substr(.SD[["code_no_dot"]], 1, x)
      ]
    }

    # Melting the dataset
    start_unique_codelist <- data.table::melt(start_unique_codelist,
      variable.name = "length_str",
      measure.vars = as.character(list_cols_names), na.rm = TRUE,
      variable.factor = FALSE,
      value.name = "code_no_dot2"
    )

    start_unique_codelist <- start_unique_codelist[
      ,
      "length_str" := as.numeric(.SD[["length_str"]])
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
    if (!all(is.na(additional_columns))) {# If it is not only NaN
      cols_by <- c(cols_by, additional_columns)
    }

    if (!is.na(priority)) {
      results_startwith2 <- data.table::copy(results_startwith)[
        order(
          -length_str,  #nolint
          eval(priority)
        ),
        .SD[1],
        by = cols_by
      ]
    } else {
      results_startwith2 <- data.table::copy(
        results_startwith
      )[order(-length_str), #nolint
        .SD[1],
        by = cols_by
      ]
    }
  }

  # Compile all results into one output
  dap_specific_codelist <- exact_match

  if (nrow(start_unique_codelist) > 0) {
    dap_specific_codelist <- data.table::rbindlist(
      list(
        dap_specific_codelist,
        results_startwith2[, .SD, .SDcols = names(start_exact_match)]
      ),
      use.names = TRUE
    )
  }

  # Finding missing codes
  missing_from_cdm <- data.table::as.data.table(
    dplyr::anti_join(
      unique_codelist, dap_specific_codelist,
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
  dap_specific_codelist[, "comment" := "BOTH"]
  dap_specific_codelist[
    is.na("code.DAP_UNIQUE_CODELIST"), "comment" := "CODELIST"
  ]
  dap_specific_codelist[is.na("code.CDM_CODELIST"), "comment" := "CDM"]

  return(dap_specific_codelist)
}

