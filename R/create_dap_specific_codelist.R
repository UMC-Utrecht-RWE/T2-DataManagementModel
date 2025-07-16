#' Create DAP-Specific Codelist
#'
#' This function merges the unique codelist from a database
#' (you can use the function getUniqueCodeList) with a study code list.
#'
#' @param unique_codelist Data.table containing unique code list output
#' from the function getUniqueCodeList. Must contain columns: 'coding_system',
#' 'code', 'concept_id', and 'code.unique_condelist'.
#' @param study_codelist Data.table containing the study code list.
#' Must contain columns: 'coding_system' and 'code'.
#' @param start_with_colls Columns to start with
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

  # Data Input Requirements Validation

  # Check if inputs are provided
  if (missing(unique_codelist) || is.null(unique_codelist)) {
    stop("unique_codelist is required and cannot be NULL or missing")
  }

  if (missing(study_codelist) || is.null(study_codelist)) {
    stop("study_codelist is required and cannot be NULL or missing")
  }

  # Ensure entries are data.table
  unique_codelist <- T2.DMM:::ensure_data_table(unique_codelist)
  study_codelist <- T2.DMM:::ensure_data_table(study_codelist)

  # Check if data.tables are not empty
  if (nrow(unique_codelist) == 0) {
    stop("unique_codelist cannot be empty")
  }

  if (nrow(study_codelist) == 0) {
    stop("study_codelist cannot be empty")
  }

  # Required columns for unique_codelist
  required_unique_cols <- c("coding_system", "code")
  missing_unique_cols <- setdiff(required_unique_cols, names(unique_codelist))
  if (length(missing_unique_cols) > 0) {
    stop(paste("unique_codelist is missing required columns:",
               paste(missing_unique_cols, collapse = ", ")))
  }

  # Required columns for study_codelist
  required_study_cols <- c("coding_system", "code", "concept_id")
  missing_study_cols <- setdiff(required_study_cols, names(study_codelist))
  if (length(missing_study_cols) > 0) {
    stop(paste("study_codelist is missing required columns:",
               paste(missing_study_cols, collapse = ", ")))
  }

  # Validate column data types and content
  if (!is.character(unique_codelist$coding_system) &&
        !is.factor(unique_codelist$coding_system)) {
    stop("unique_codelist$coding_system must be character or factor")
  }

  if (!is.character(study_codelist$coding_system) &&
        !is.factor(study_codelist$coding_system)) {
    stop("study_codelist$coding_system must be character or factor")
  }

  if (!is.character(unique_codelist$code) && !is.factor(unique_codelist$code)) {
    stop("unique_codelist$code must be character or factor")
  }

  if (!is.character(study_codelist$code) && !is.factor(study_codelist$code)) {
    stop("study_codelist$code must be character or factor")
  }

  # Check for NA values in critical columns
  if (any(is.na(unique_codelist$coding_system))) {
    warning("unique_codelist contains NA values in coding_system column")
  }

  if (any(is.na(study_codelist$coding_system))) {
    warning("study_codelist contains NA values in coding_system column")
  }

  if (any(is.na(unique_codelist$code))) {
    warning("unique_codelist contains NA values in code column")
  }

  if (any(is.na(study_codelist$code))) {
    warning("study_codelist contains NA values in code column")
  }

  # Validate priority parameter
  if (!is.na(priority)) {
    if (!is.character(priority) || length(priority) != 1) {
      stop("priority must be a single character string or NA")
    }

    if (!priority %in% names(unique_codelist)) {
      stop(paste(
        "priority column '",
        priority,
        "' not found in unique_codelist"
      ))
    }
  }

  # Validate start_with_colls parameter
  if (!is.character(start_with_colls)) {
    stop("start_with_colls must be a character vector")
  }

  # Preprocessing the study_codelist
  study_codelist[, code_no_dot := gsub("\\.", "", code)]
  study_codelist[, length_str := stringr::str_length(code_no_dot)]
  data.table::setnames(study_codelist, "code", "code.study_codelist")
  min_length_study_codelist <- min(study_codelist$length_str, na.rm = TRUE)

  # Preprocessing the unique_codelist
  unique_codelist[, code_no_dot := gsub("\\.", "", code)]
  data.table::setnames(unique_codelist, "code", "code.unique_codelist")

  # Dividing code lists into exact and start with codes
  start_unique_codelist <- unique_codelist[coding_system %in% start_with_colls]
  start_study_codelist <- study_codelist[coding_system %in% start_with_colls]
  exact_unique_codelist <- unique_codelist[!coding_system %in% start_with_colls]
  exact_study_codelist <- study_codelist[!coding_system %in% start_with_colls]

  # Finding exact matches
  exact_match <- NULL
  if (nrow(exact_unique_codelist) > 0) {
    exact_match <- merge(
      exact_unique_codelist, exact_study_codelist,
      by = c("coding_system", "code_no_dot")
    )
  }

  if (nrow(start_unique_codelist) > 0) {
    # Finding start with matches
    # TODO check merge is intersect join; result is a smaller set
    start_exact_match <- merge(
      start_unique_codelist, start_study_codelist,
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
    start_unique_codelist <- data.table::melt(
      start_unique_codelist,
      variable.name = "length_str",
      measure.vars = as.character(list_cols_names),
      na.rm = TRUE,
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
    cols_by <- c("code.unique_codelist", "concept_id", "coding_system")

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
  dap_specific_codelist[is.na(code.unique_codelist), Comment := "CODELIST"]
  dap_specific_codelist[is.na(code.study_codelist), Comment := "CDM"]

  return(dap_specific_codelist)
}