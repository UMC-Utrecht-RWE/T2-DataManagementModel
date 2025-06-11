#' Create DAP-Specific Concepts into Database
#'
#' This function Creates DAP-specific concepts into a database using a
#' DAP-specifc concept map (codelist).
#' The function modifies the specified save_db by creating edited tables
#' and Creating DAP-specific concepts.
#'
#' @param codelist A data.table containing information about tables,
#' columns, and values for DAP-specific concepts.
#' @param name_attachment Attachment to the database table names.
#' @param save_db The database connection object where the edited tables
#' and concepts will be saved.
#' @param date_col_filter An optional filter to subset data based on
#' a specified date column.
#' @param table_name Name of the table in the codelist.
#' Default: "cdm_table_name".
#' @param column_name_prefix An optional string that defines the prefix name
#' of the column name variable column(s) from the DAP-specific concept map.
#' Default: "column_name".
#' @param expected_value_prefix An optional string that defines the prefix name
#'  of the expected value variable column(s) from the DAP-specific concept map.
#' Default: "expected_value".
#' @param add_meaning An optional boolean that defines whether the possibility
#'  to save the meaning of any CDM table -if available- in the results of the
#' function. This is specific for the ConcePTION CDM.
#' Default: FALSE.
#'
#' @export
create_dap_specific_concept <- function(
    codelist,
    name_attachment,
    save_db,
    date_col_filter = NULL,
    table_name = "cdm_table_name",
    column_name_prefix = "column_name",
    expected_value_prefix = "expected_value",
    add_meaning = FALSE) {
  if (nrow(codelist) <= 0) {
    stop("Codelist does not contain any data.")
  }
  scheme <- unique(codelist[[table_name]])
  cols_names <- grep(paste0("^", column_name_prefix), names(codelist),
    value = TRUE
  )
  value_names <- grep(paste0("^", expected_value_prefix),
    names(codelist),
    value = TRUE
  )
  cols <- codelist[, ..cols_names]
  values <- codelist[, ..value_names]
  for (name in scheme) {
    name_edited <- paste0(name, "_EDITED")
    to_upper_cols <- na.omit(
      unique(unlist(codelist[get(table_name) %in% name, ..cols_names]))
    )
    query_columns_table <- paste0("
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '", name, "'
          ")
    columns_db_table <- DBI::dbGetQuery(
      save_db, query_columns_table
    )$column_name

    rest_cols <- na.omit(columns_db_table[!columns_db_table %in% to_upper_cols])
    to_upper_query <- paste0(paste0(
      "UPPER(", to_upper_cols,
      ") AS ", to_upper_cols
    ), collapse = ", ")
    select_cols_query <- paste0(
      paste0(rest_cols, collapse = ", "),
      " ,"
    )
    if (!name_edited %in% DBI::dbListTables(save_db)) {
      rs <- DBI::dbSendStatement(save_db, paste0(
        "CREATE TEMP TABLE ",
        name_edited, " AS\n              SELECT ", select_cols_query,
        " ", to_upper_query, "\n              FROM ",
        name_attachment, ".", name
      ))
      DBI::dbClearResult(rs)
    } else if (all(c(rest_cols, to_upper_cols) %in% DBI::dbListFields(
      save_db,
      name_edited
    )) == FALSE) {
      rs <- DBI::dbSendStatement(save_db, paste0(
        "CREATE TEMP TABLE ",
        name_edited, " AS\n              SELECT ", select_cols_query,
        " ", to_upper_query, "\n              FROM ",
        name_attachment, ".", name
      ))
      DBI::dbClearResult(rs)
    }
  }
  for (j in seq_len(nrow(codelist))) {
    table_temp <- codelist[[j, table_name]]
    name_edited <- paste0(table_temp, "_edited")
    concept_name <- codelist[[j, "concept_id"]]
    date_col <- codelist[[j, "keep_date_column_name"]]
    codelist_id <- codelist[[j, "dap_spec_id"]]
    cols_temp <- na.omit(as.character(cols[j]))
    values_temp <- toupper(na.omit(as.character(values[j])))
    value <- codelist[[j, "keep_value_column_name"]]
    if (add_meaning) {
      columns_db_table <- DBI::dbListFields(save_db, name_edited)
      meaning_column_name <- columns_db_table[stringr::str_detect(
        columns_db_table,
        "meaning"
      )]
      if (length(meaning_column_name) > 0) {
        meaning_clause <- paste0(
          ", ", meaning_column_name,
          " AS meaning "
        )
      } else {
        message(paste0(
          "[create_dap_specific_concept] Meaning not identified for: ",
          name_edited
        ))
        meaning_clause <- paste0(", NULL AS meaning ")
      }
    } else {
      meaning_clause <- ""
    }
    if (is.null(value)) {
      value <- TRUE
    } else if (any(is.na(value))) {
      value <- TRUE
    }
    if (is.null(date_col)) {
      date_col <- "'NA'"
    } else if (any(is.na(date_col))) {
      date_col <- "'NA'"
    }
    coding_system <- paste0("'", codelist_id, "'")
    if (class(save_db)[1] %in% "duckdb_connection") {
      where_statement <- paste(paste(cols_temp, paste0(
        "'",
        values_temp, "'"
      ), sep = " = "), collapse = " AND ")
      if (!is.null(date_col_filter)) {
        where_statement <- paste0(
          where_statement, " AND ",
          date_col, " >= DATE '", date_col_filter, "'"
        )
      }
    } else {
      where_statement <- paste(paste(cols_temp, paste0(
        "'",
        values_temp, "'"
      ), sep = " = "), collapse = " AND ")
      if (!is.null(date_col_filter)) {
        where_statement <- paste0(
          where_statement, " AND ",
          date_col, " >= ", as.integer(date_col_filter)
        )
      }
    }
    rs <- DBI::dbSendStatement(save_db, paste0(
      "INSERT INTO concept_table\n
      SELECT t1.ori_id, t1.ori_table, ROWID, t1.person_id, ",
      coding_system, " AS code, ", coding_system, " AS coding_system, ",
      value, " AS value, '", concept_name, "' AS concept_id, ",
      date_col, " AS date ", meaning_clause, "FROM ",
      name_edited, " t1", " WHERE ", where_statement
    ))
    DBI::dbClearResult(rs)
  }
}
