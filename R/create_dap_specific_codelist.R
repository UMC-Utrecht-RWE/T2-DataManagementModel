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
  start_with_cols = c(
    "ICD10CM", "ICD10", "ICD10DA", "ICD9CM", "MTHICD9",
    "ICPC", "ICPC2P", "ICPC2EENG", "ATC", "vx_atc"
  ),
  additional_columns = NA,
  priority = NA
) {
  # Check inputs
  if (!is.data.table(unique_codelist))
    stop("unique_codelist must be a data.table")
  if (!is.data.table(study_codelist))
    stop("study_codelist must be a data.table")
  if (!is.character(start_with_cols) || !is.vector(start_with_cols)) {
    stop("start_with_cols must be a character vector (list of strings)")
  }

  # Preprocessing the study_codelist
  study_codelist <- study_codelist %>%
    dplyr::mutate(code_no_dot = gsub("\\.", "", .data$code)) %>%
    dplyr::mutate(
      length_str = stringr::str_length(.data$code_no_dot)
    ) %>%
    #This is flag that marks the origin of the rows
    dplyr::mutate(code.CDM_CODELIST = .data$code)

  min_length_study_codelist <- min(study_codelist$length_str, na.rm = TRUE)

  # Dividing code lists into exact matches and start with codes
  # The names refer to further analysis.
  # Here, we get the rows that match start_with_cols and not.
  start_unique <- unique_codelist %>%
    dplyr::filter(unique_codelist$coding_system %in% start_with_cols)
  start_study <- study_codelist %>%
    dplyr::filter(study_codelist$coding_system %in% start_with_cols)

  # Perfect match
  exact_unique <- unique_codelist %>%
    dplyr::filter(!unique_codelist$coding_system %in% start_with_cols)
  exact_study <- study_codelist %>%
    dplyr::filter(!study_codelist$coding_system %in% start_with_cols)

  print(c( #TODO: It should be logged.
    dim(start_unique),
    dim(start_study),
    dim(exact_unique),
    dim(exact_study)
  ))

  #########################################
  # Finding exact matches: aka, merging imput file without cols.
  #########################################
  if (nrow(exact_unique) > 0) {
    exact_match <- exact_unique %>%
      dplyr::inner_join(
        exact_study,
        by = c("coding_system", "code_no_dot")
      )
  } else {
    exact_match <- NULL
  }

  #########################################
  # Finding Start Unique
  #########################################
  if (nrow(start_unique) > 0) {
    # Finding start with matches
    start_match <- start_unique %>%
      dplyr::inner_join(
        start_study,
        by = c("coding_system", "code_no_dot")
      )

    # Preprocessing start with codes
    start_unique <- start_unique %>%
      dplyr::mutate(ori_length_str = stringr::str_length(.data$code_no_dot))

    max_length <- max(start_unique$ori_length_str)

    print(paste0(
      "[SetCodesheets] Max length of code from the DAP is : ",
      max_length
    ))

    # Splitting start with codes into different lengths
    list_cols_names <- seq(min_length_study_codelist, max_length)
    # Create a new column for each value of list
    # cols_names as increasing window of code.DAP_UNIQUE_CODELIST. As the follow
    # | code_no_dot |   3  |   4  |    5   |
    # |-------------|------|------|--------|
    # |    R229     | R22  | R229 |   NA   |
    # |    M7910    | M79  | M791 |  M7910 |
    for (x in list_cols_names) {
      rows <- start_unique$ori_length_str >= x
      start_unique[
        rows, (as.character(x)) := substr(.SD[["code_no_dot"]], 1, x)
      ]
    }

    # Melting the dataset to obtain the following:
    # | code_no_dot | length_str | code_no_dot2 |
    # |-------------|------------|--------------|
    # |    R229     |     3      |     R22      |
    # |    R229     |     4      |     R229     |
    # |    M7910    |     3      |     M79      |
    # |    M7910    |     4      |     M791     |
    # |    M7910    |     5      |     M7910    |
    start_unique <- data.table::melt(start_unique,
      variable.name = "length_str",
      measure.vars = as.character(list_cols_names),
      na.rm = TRUE,
      variable.factor = FALSE,
      value.name = "code_no_dot2"
    )
    start_unique <- start_unique[
      ,
      "length_str" := as.numeric(.SD[["length_str"]])
    ]

    # Inner joining start with codes with the study codelist
    results_startwith <- merge(
      x = start_unique,
      y = start_study,
      by.x = c("coding_system", "code_no_dot2", "length_str"),
      by.y = c("coding_system", "code_no_dot", "length_str")
    )

    # Selecting the top priority matches
    cols_by <- c("code.DAP_UNIQUE_CODELIST", "concept_id", "coding_system")
    if (!all(is.na(additional_columns))) {# If it is not only NaN
      cols_by <- c(cols_by, additional_columns)
    }

    if (!is.na(priority)) { #Is ordering something needed?
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
  dap_specific_codelist <- data.table::copy(exact_match)

  if (nrow(start_unique) > 0) {
    dap_specific_codelist <- data.table::rbindlist( #Row-wise merging
      list(
        dap_specific_codelist,
        results_startwith2[, .SD, .SDcols = names(start_match)]
      ),
      use.names = TRUE
    )
  }

  # Finding missing codes
  # With the following two anti_join we get what we lost
  # during the inner join of results_startwith
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
  ## Both if the codelist is present on both,
  ## or CODELIST or CDM, otherwise.
  dap_specific_codelist[, "comment" := "BOTH"]
  dap_specific_codelist[
    is.na(dap_specific_codelist$code.DAP_UNIQUE_CODELIST),
    "comment" := "CODELIST"
  ]
  dap_specific_codelist[
    is.na(dap_specific_codelist$code.CDM_CODELIST),
    "comment" := "CDM"
  ]

  return(dap_specific_codelist)
}
